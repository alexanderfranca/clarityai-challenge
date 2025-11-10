from pprint import pprint
import sys
import csv
import hashlib
import fnmatch
import yaml
from typing import (
        Dict,
        List,
        Any,
        Tuple,
)
from pathlib import Path
from utils.log import init_logger
from datetime import datetime, timezone


CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
DATA_INCOMING = Path(__file__).resolve().parents[1] / "data" / "incoming"
BRONZE_ROOT = Path(__file__).resolve().parents[1] / "data" / "bronze"
logger = init_logger()


def load_yaml(name: str) -> dict:
    """
    Load and validate a YAML config file.
    """
    path = CONFIG_DIR / name
    if not path.exists():
        sys.exit(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        sys.exit(f"YAML syntax error in {name}: {e}")

    if not isinstance(data, dict):
        sys.exit(f"Invalid format in {name}: expected mapping at top level")

    return data


def discover_batches(paths_cfg: dict) -> Dict[str, List[str]]:
    """
    Scan provider incoming folders defined in paths.yaml for ready batches.
    A batch is ready if it has a '_READY' marker (or quarantine elapsed).
    """
    result: Dict[str, List[str]] = {}
    for provider, cfg in paths_cfg.get("providers", {}).items():
        incoming_dir = Path(
                cfg.get("incoming_dir", f"data/incoming/{provider}")
                ).resolve()
        readiness = cfg.get("readiness", {}).get("marker_file", "_READY")
        quarantine_sec = cfg.get("readiness", {}).get("quarantine_seconds", 0)

        if not incoming_dir.exists():
            logger.warning(f"Provider path missing: {incoming_dir}")
            continue

        ready_batches: List[str] = []

        for batch_dir in sorted(incoming_dir.iterdir()):
            if not batch_dir.is_dir():
                continue

            marker = batch_dir / readiness
            if marker.exists():
                ready_batches.append(batch_dir.name)
                logger.info(f"Batch ready: {provider}/{batch_dir.name}")
                continue

            if quarantine_sec > 0:
                age = (
                        datetime.now(timezone.utc) - datetime.fromtimestamp(
                            batch_dir.stat().st_mtime, tz=timezone.utc)
                        ).total_seconds()
                if age >= quarantine_sec:
                    logger.info(
                            "Quarantine passed: "
                            f"{provider}/{batch_dir.name}"
                            )
                    ready_batches.append(batch_dir.name)

        if ready_batches:
            result[provider] = ready_batches

    if not result:
        logger.warning("No ready batches found.")
    else:
        total = sum(len(v) for v in result.values())
        logger.info(f"Discovered {total} ready batch(es).")

    return result


def build_plan(batches: Dict[str, List[str]], mappings_cfg: dict) -> List[Dict[str, Any]]:
    """
    Build a plan of execution.
    The plan is a relation between source files and entities/feeds.
    """
    plan = []
    for provider, batch_ids in batches.items():
        provider_cfg = mappings_cfg.get("providers", {}).get(provider)
        if not provider_cfg:
            logger.warning(f"No mapping found for provider: {provider}")
            continue

        feeds = provider_cfg.get("feeds", {})
        if not feeds:
            logger.warning(f"No feeds found for provider: {provider}")
            continue

        for batch_id in batch_ids:
            batch_path = Path("data/incoming") / provider / batch_id
            for file_path in sorted(batch_path.glob("*")):
                if not file_path.is_file() or file_path.name.startswith("_"):
                    continue

                matched_feed = None
                matched_entity = None
                for feed_name, feed_cfg in feeds.items():
                    file_name_pattern = feed_cfg.get("filename_selector", {})
                    if not file_name_pattern:
                        logger.warning(
                                f"Feed {feed_name} "
                                "missing selector pattern."
                        )
                        continue

                    if fnmatch.fnmatch(file_path.name, file_name_pattern):
                        matched_feed = feed_name
                        matched_entity = feed_cfg.get("target_entity")
                        break

                if matched_feed:
                    plan.append(
                            {
                                "provider": provider,
                                "batch_id": batch_id,
                                "file": file_path,
                                "feed": matched_feed,
                                "target_entity": matched_entity,
                            }
                    )
                    logger.info(
                        "Plan created for: "
                        f"{file_path.name} -> {matched_entity}"
                    )
                else:
                    logger.warning(f"File didn't match any: {file_path.name}")

    if not plan:
        logger.warning("No files matched any feed pattern.")
    else:
        logger.info(
                "Planned: "
                f"{len(plan)} file(s) total."
        )

    return plan


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _csv_header(
        path: Path,
        delimiter: str = ",",
        encoding: str = "utf-8") -> List[str]:

    with path.open("r", encoding=encoding, newline="") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)

        if not delimiter:
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = ","
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, None)
        if not header:
            raise ValueError("File is empty or missing header")
        return [h.strip() for h in header]


def _required_source_cols(feed_cfg: Dict[str, Any]) -> List[str]:
    mappings = feed_cfg.get("mappings", {})
    return sorted(list(mappings.keys()))


def _csv_options(feed_cfg: Dict[str, Any]) -> Tuple[str, str]:
    opts = feed_cfg.get("csv_options", {})
    delimiter = opts.get("delimiter", ",")
    encoding = opts.get("encoding", "utf-8")
    return delimiter, encoding


def precheck_and_schema_gate(
    plan: List[Dict[str, Any]],
    mappings_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    For each planned file:
      - checks existence & size
      - computes file_hash
      - validates header has all required columns per mapping
      - records additive columns as drift (warn, continue)
    Returns a filtered list of plan items enriched with metadata for ingestion.
    """
    qualified: List[Dict[str, Any]] = []

    providers_cfg = mappings_cfg.get("providers", {})

    for item in plan:
        provider = item["provider"]
        feed = item["feed"]
        file_path = Path(item["file"])
        feed_cfg = providers_cfg.get(provider, {}).get("feeds", {}).get(feed)

        if not feed_cfg:
            logger.error(f"Missing feed config: {provider}.{feed}")
            continue

        if feed_cfg.get("input_format", "csv").lower() != "csv":
            logger.error(
                    "Only CSV supported. "
                    f"Got {provider}.{feed} as non-CSV)."
            )
            continue

        if not file_path.exists() or not file_path.is_file():
            logger.error(f"File not found: {file_path}")
            continue
        size_bytes = file_path.stat().st_size

        if size_bytes <= 0:
            logger.error(f"Empty file: {file_path}")
            continue

        source_mod_time = datetime.fromtimestamp(
                file_path.stat().st_mtime, tz=timezone.utc
        )

        file_hash = _sha256_file(file_path)

        delimiter, encoding = _csv_options(feed_cfg)
        try:
            header = _csv_header(file_path, delimiter=delimiter, encoding=encoding)
        except Exception as e:
            logger.error(f"Cannot read header for {file_path.name}: {e}")
            continue

        required = _required_source_cols(feed_cfg)
        missing = [c for c in required if c not in header]
        extra = [c for c in header if c not in required]

        if missing:
            logger.error(
                f"Schema gate failed: "
                f"{file_path.name} is missing required columns: {missing}"
            )
            continue

        if extra:
            logger.warning(
                f"Additive columns detected in {file_path.name}: "
                f"{extra} (allowed in bronze)"
            )

        try:
            with file_path.open("r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                next(reader, None)
        except Exception as e:
            logger.error(f"CSV parse sanity failed for {file_path.name}: {e}")
            continue

        enriched = dict(item)
        enriched.update({
            "size_bytes": size_bytes,
            "source_mod_time": source_mod_time.isoformat(),
            "file_hash": file_hash,
            "header": header,
            "required_cols": required,
            "extra_cols": extra,
        })
        qualified.append(enriched)
        logger.info(
                f"Precheck passed: {file_path.name} ({size_bytes} bytes)"
        )

    if not qualified:
        logger.error("No files passed precheck/schema gate.")
    else:
        logger.info(
                "Precheck complete: "
                f"{len(qualified)}/{len(plan)} file(s) qualified."
        )

    return qualified


def _collapse_ws(s: str) -> str:
    return " ".join(s.strip().split())


def _derive_movie_key_v1(title: str, year: int) -> str:
    norm = f"{_collapse_ws(title).lower()}|{year}"
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def _row_record_hash_domestic(row: dict) -> str:
    # business identity for this feed after normalization/mapping
    payload = f"{row.get('title','')}|{row.get('year','')}|{row.get('domestic_box_office_gross','')}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _nonneg_int(x):
    try:
        v = int(x)
        return v if v >= 0 else None
    except Exception:
        return None


def ingest_domestic_csv(item: dict, mappings_cfg: dict) -> None:
    """
    Ingest ONE domestic CSV file to bronze:
      - map/normalize: 
            film_name->title,
            year_of_release->year,
            box_office_gross_usd->domestic_box_office_gross
      - derive movie_key
      - compute record_hash
      - dedupe within-file on (movie_key, record_hash)
      - stamp bronze metadata
      - write to:
        data/bronze/boxofficemetrics/domestic_csv/<batch_id>/part-0001.csv
    """
    provider = item["provider"]
    batch_id = item["batch_id"]
    feed = item["feed"]
    if feed != "domestic_csv":
        return  # only domestic in step 1.5

    file_path = Path(item["file"])
    providers_cfg = mappings_cfg["providers"]
    feed_cfg = providers_cfg[provider]["feeds"][feed]
    csv_opts = feed_cfg.get("csv_options", {})
    delimiter = csv_opts.get("delimiter", ",")
    encoding = csv_opts.get("encoding", "utf-8")
    schema_version = providers_cfg[provider].get("schema_version", 1)

    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0001.csv"

    out_columns = [
        # bronze metadata
        "provider",
        "feed",
        "batch_id",
        "source_file",
        "source_mod_time",
        "file_hash",
        "ingest_ts",
        "schema_version",
        "record_hash",
        # business columns for this feed
        "movie_key",
        "title",
        "year",
        "domestic_box_office_gross",
    ]

    seen = set()
    rows_out = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()

    with file_path.open("r", encoding=encoding, newline="") as fin, out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin, delimiter=delimiter)
        writer = csv.DictWriter(fout, fieldnames=out_columns)
        writer.writeheader()

        for raw in reader:
            title_raw = raw.get("film_name", "")
            year_raw = raw.get("year_of_release", "")
            gross_raw = raw.get("box_office_gross_usd", "")

            title = _collapse_ws(title_raw)
            try:
                year = int(year_raw)
            except Exception:
                logger.warning(
                        "Drop row (bad year): "
                        f"{file_path.name} -> {year_raw!r}"
                )
                continue
            if year < 1900 or year > 2035:
                logger.warning(
                        "Drop row (year out of range): "
                        f"{file_path.name} -> {year}"
                )
                continue

            domestic_box_office_gross = _nonneg_int(gross_raw)
            movie_key = _derive_movie_key_v1(title, year)

            business_row = {
                "movie_key": movie_key,
                "title": title,
                "year": year,
                "domestic_box_office_gross": domestic_box_office_gross,
            }
            record_hash = _row_record_hash_domestic(business_row)
            dedupe_key = (movie_key, record_hash)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            writer.writerow({
                "provider": provider,
                "feed": feed,
                "batch_id": batch_id,
                "source_file": file_path.name,
                "source_mod_time": item["source_mod_time"],
                "file_hash": item["file_hash"],
                "ingest_ts": ingest_ts,
                "schema_version": schema_version,
                "record_hash": record_hash,
                **business_row,
            })
            rows_out += 1

    logger.info(f"Wrote bronze: {out_path} (rows={rows_out})")


def main() -> None:
    """
    Entry point of the pipeline.
    """
    logger.info("Loading pipeline contracts and configs.")

    contracts = load_yaml("contracts.yaml")
    mappings = load_yaml("mappings.yaml")
    paths = load_yaml("paths.yaml")

    logger.info(f"- contracts: {len(contracts)} top-level keys")
    logger.info(f"- mappings:  {len(mappings)} top-level keys")
    logger.info(f"- paths:     {len(paths)} top-level keys")
    logger.info("All contracts and configs loadded successfully.")

    batches = discover_batches(paths)
    logger.info(f"Found batches: {batches}")

    plan = build_plan(batches, mappings)

    qualified_plan = precheck_and_schema_gate(plan, mappings)
    logger.info(f"Qualified plan size: {len(qualified_plan)}")

    for q in qualified_plan:
        ingest_domestic_csv(q, mappings)


if __name__ == "__main__":
    main()

