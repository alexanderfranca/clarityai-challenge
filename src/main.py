from pprint import pprint
import sys
import csv
import hashlib
import fnmatch
import yaml
import json
from collections import defaultdict
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
AUDIT_LEDGER = Path(__file__).resolve().parents[1] / "audit" / "ledger.jsonl"
AUDIT_LEDGER.parent.mkdir(parents=True, exist_ok=True)

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


def _sample_json_records(path: Path, max_records: int = 50):
    # Try JSON Lines first; fallback to a single JSON array
    with path.open("r", encoding="utf-8") as f:
        first = f.read(2048)
        f.seek(0)
        if first.lstrip().startswith("["):  # JSON array
            import json as _json
            arr = _json.load(f)
            for rec in arr[:max_records]:
                if isinstance(rec, dict):
                    yield rec
            return
        # JSONL
        f.seek(0)
        count = 0
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                import json as _json
                rec = _json.loads(line)
                if isinstance(rec, dict):
                    yield rec
                    count += 1
                    if count >= max_records:
                        break
            except Exception:
                raise ValueError("Invalid JSON/JSONL content")


def precheck_and_schema_gate_json(item: Dict[str, Any], feed_cfg: Dict[str, Any]) -> Dict[str, Any] | None:
    file_path = Path(item["file"])
    if not file_path.exists() or not file_path.is_file() or file_path.stat().st_size <= 0:
        logger.error(f"File missing/empty: {file_path}")
        return None

    file_hash = _sha256_file(file_path)
    if already_processed(item["provider"], item["batch_id"], file_path.name, file_hash):
        logger.info(f"⏭️  Skip (already processed): {file_path.name}")
        return None

    required = set(_required_source_cols(feed_cfg))
    seen_keys = set()
    try:
        for rec in _sample_json_records(file_path, max_records=50):
            seen_keys.update(rec.keys())
    except Exception as e:
        logger.error(f"Cannot sample JSON for {file_path.name}: {e}")
        return None

    missing = sorted(list(required - seen_keys))
    extra = sorted(list(seen_keys - required))
    if missing:
        logger.error(f"Schema gate failed (JSON): {file_path.name} missing {missing}")
        return None
    if extra:
        logger.warning(f"⚠️  Additive fields detected in {file_path.name}: {extra} (allowed in bronze)")

    enriched = dict(item)
    enriched.update({
        "size_bytes": file_path.stat().st_size,
        "source_mod_time": datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc).isoformat(),
        "file_hash": file_hash,
        "header": sorted(list(seen_keys)),
        "required_cols": sorted(list(required)),
        "extra_cols": extra,
    })
    logger.info(f"✅ JSON precheck passed: {file_path.name}")
    return enriched


def _apply_mappings(row: dict, feed_cfg: dict) -> dict:
    out = {}
    for src, spec in feed_cfg.get("mappings", {}).items():
        tgt = spec["to"]; typ = (spec.get("type") or "string").lower()
        val = row.get(src)
        if val is None:
            out[tgt] = None
            continue
        if typ == "int":
            try: out[tgt] = int(val)
            except: out[tgt] = None
        elif typ in ("string", "str"):
            out[tgt] = _collapse_ws(str(val))
        else:
            out[tgt] = val
    return out

def ingest_json_generic(item: dict, mappings_cfg: dict) -> None:
    if mappings_cfg["providers"][item["provider"]]["feeds"][item["feed"]].get("input_format","csv").lower() != "json":
        return

    provider = item["provider"]; batch_id = item["batch_id"]; feed = item["feed"]
    feed_cfg = mappings_cfg["providers"][provider]["feeds"][feed]
    schema_version = mappings_cfg["providers"][provider].get("schema_version", 1)
    file_path = Path(item["file"])

    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{item['file_hash'][:12]}.csv"

    # Determine target columns from mappings + bronze metadata
    target_cols = [spec["to"] for spec in feed_cfg.get("mappings", {}).values()]
    out_columns = [
        "provider","feed","batch_id","source_file","source_mod_time","file_hash","ingest_ts","schema_version","record_hash",
        *target_cols
    ]

    seen = set(); rows_out = 0; rows_in = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()

    with file_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=out_columns); writer.writeheader()

        for rec in _sample_json_records(file_path, max_records=10_000_000):
            rows_in += 1
            mapped = _apply_mappings(rec, feed_cfg)

            # Optional: derive movie_key only if you mapped title+year
            if "movie_key" in target_cols:
                pass  # you mapped it yourself
            elif "title" in mapped and "year" in mapped and mapped["year"] is not None:
                mapped["movie_key"] = _derive_movie_key_v1(mapped["title"], mapped["year"])
                if "movie_key" not in target_cols:
                    out_columns.append("movie_key")  # future-proof if needed

            # Build record hash from the mapped fields (stable order)
            payload = "|".join(str(mapped.get(c, "")) for c in sorted(target_cols))
            record_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            dedupe_key = (mapped.get("movie_key"), record_hash)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            writer.writerow({
                "provider": provider, "feed": feed, "batch_id": batch_id,
                "source_file": file_path.name, "source_mod_time": item["source_mod_time"],
                "file_hash": item["file_hash"], "ingest_ts": ingest_ts,
                "schema_version": schema_version, "record_hash": record_hash,
                **mapped,
            })
            rows_out += 1

    write_audit(provider=provider, batch_id=batch_id, feed=feed,
                source_file=file_path.name, file_hash=item["file_hash"],
                rows_in=rows_in, rows_out=rows_out, status="ok")
    logger.info(f"🟢 Wrote bronze(JSON): {out_path} (rows_in={rows_in}, rows_out={rows_out})")


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

        if already_processed(
                provider,
                item["batch_id"],
                file_path.name,
                file_hash):
            logger.info(f"Skip (already processed): {file_path.name}")
            continue

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
    file_hash_file_name = f"{item['file_hash'][:12]}.csv"
    out_path = out_dir / file_hash_file_name

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

        rows_in = 0
        for raw in reader:
            rows_in += 1
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

    write_audit(
        provider=provider, batch_id=batch_id, feed=feed,
        source_file=file_path.name, file_hash=item["file_hash"],
        rows_in=rows_in, rows_out=rows_out, status="ok"
    )
    logger.info(f"Wrote bronze: {out_path} (rows={rows_out})")


def _row_record_hash_international(row: dict) -> str:
    payload = f"{row.get('movie_key','')}|{row.get('title','')}|{row.get('year','')}|{row.get('international_box_office_gross','')}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_record_hash_financials(row: dict) -> str:
    payload = f"{row.get('movie_key','')}|{row.get('title','')}|{row.get('year','')}|{row.get('production_budget_usd','')}|{row.get('marketing_spend_usd','')}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ingest_international_csv(item: dict, mappings_cfg: dict) -> None:
    if item["feed"] != "international_csv":
        return

    provider = item["provider"]; batch_id = item["batch_id"]; feed = item["feed"]
    file_path = Path(item["file"])
    providers_cfg = mappings_cfg["providers"]
    feed_cfg = providers_cfg[provider]["feeds"][feed]
    csv_opts = feed_cfg.get("csv_options", {})
    delimiter = csv_opts.get("delimiter", ","); encoding = csv_opts.get("encoding", "utf-8")
    schema_version = providers_cfg[provider].get("schema_version", 1)

    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    file_hash_file_name = f"{item['file_hash'][:12]}.csv"
    out_path = out_dir / file_hash_file_name

    out_columns = [
        "provider","feed","batch_id","source_file","source_mod_time","file_hash","ingest_ts","schema_version","record_hash",
        "movie_key","title","year","international_box_office_gross",
    ]

    seen = set(); rows_out = 0; rows_in = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()

    with file_path.open("r", encoding=encoding, newline="") as fin, out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin, delimiter=delimiter)
        writer = csv.DictWriter(fout, fieldnames=out_columns); writer.writeheader()

        rows_in = 0
        for raw in reader:
            rows_in += 1
            title = _collapse_ws(raw.get("film_name", ""))
            try:
                year = int(raw.get("year_of_release", ""))
            except Exception:
                logger.warning(f"Drop row (bad year): {file_path.name} -> {raw.get('year_of_release')!r}"); continue
            if year < 1900 or year > 2035:
                logger.warning(f"Drop row (year out of range): {file_path.name} -> {year}"); continue

            international_box_office_gross = _nonneg_int(raw.get("box_office_gross_usd", ""))

            movie_key = _derive_movie_key_v1(title, year)
            business_row = {
                "movie_key": movie_key,
                "title": title,
                "year": year,
                "international_box_office_gross": international_box_office_gross,
            }
            record_hash = _row_record_hash_international(business_row)
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

    write_audit(
        provider=provider, batch_id=batch_id, feed=feed,
        source_file=file_path.name, file_hash=item["file_hash"],
        rows_in=rows_in, rows_out=rows_out, status="ok"
    )
    logger.info(
            f"Wrote bronze: {out_path} "
            f"(rows_in={rows_in}, rows_out={rows_out})"
    )


def ingest_financials_csv(item: dict, mappings_cfg: dict) -> None:
    if item["feed"] != "financials_csv":
        return

    provider = item["provider"]; batch_id = item["batch_id"]; feed = item["feed"]
    file_path = Path(item["file"])
    providers_cfg = mappings_cfg["providers"]
    feed_cfg = providers_cfg[provider]["feeds"][feed]
    csv_opts = feed_cfg.get("csv_options", {})
    delimiter = csv_opts.get("delimiter", ","); encoding = csv_opts.get("encoding", "utf-8")
    schema_version = providers_cfg[provider].get("schema_version", 1)

    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    file_hash_file_name = f"{item['file_hash'][:12]}.csv"
    out_path = out_dir / file_hash_file_name

    out_columns = [
        "provider","feed","batch_id","source_file","source_mod_time","file_hash","ingest_ts","schema_version","record_hash",
        "movie_key","title","year","production_budget_usd","marketing_spend_usd",
    ]

    seen = set(); rows_out = 0; rows_in = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()

    with file_path.open("r", encoding=encoding, newline="") as fin, out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin, delimiter=delimiter)
        writer = csv.DictWriter(fout, fieldnames=out_columns); writer.writeheader()

        rows_in = 0
        for raw in reader:
            rows_in += 1
            title = _collapse_ws(raw.get("film_name", ""))
            try:
                year = int(raw.get("year_of_release", ""))
            except Exception:
                logger.warning(f"Drop row (bad year): {file_path.name} -> {raw.get('year_of_release')!r}"); continue
            if year < 1900 or year > 2035:
                logger.warning(f"Drop row (year out of range): {file_path.name} -> {year}"); continue

            production_budget_usd = _nonneg_int(raw.get("production_budget_usd", ""))
            marketing_spend_usd   = _nonneg_int(raw.get("marketing_spend_usd", ""))

            movie_key = _derive_movie_key_v1(title, year)
            business_row = {
                "movie_key": movie_key,
                "title": title,
                "year": year,
                "production_budget_usd": production_budget_usd,
                "marketing_spend_usd": marketing_spend_usd,
            }
            record_hash = _row_record_hash_financials(business_row)
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

    write_audit(
        provider=provider, batch_id=batch_id, feed=feed,
        source_file=file_path.name, file_hash=item["file_hash"],
        rows_in=rows_in, rows_out=rows_out, status="ok"
    )
    logger.info(f"Wrote bronze: {out_path} (rows_in={rows_in}, rows_out={rows_out})")


def already_processed(provider: str, batch_id: str, source_file: str, file_hash: str) -> bool:
    if not AUDIT_LEDGER.exists():
        return False
    key = (provider, batch_id, source_file, file_hash)
    with AUDIT_LEDGER.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                if (rec.get("provider"), rec.get("batch_id"), rec.get("source_file"), rec.get("file_hash")) == key:
                    return True
            except Exception:
                continue
    return False


def write_audit(**kwargs):
    rec = dict(kwargs)
    rec.setdefault("ts", datetime.now(timezone.utc).isoformat())
    with AUDIT_LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def consolidate_bronze_feed(provider: str, feed: str, batch_id: str):
    base = BRONZE_ROOT / provider / feed / batch_id
    parts = sorted(
            p for p in base.glob("*.csv") if p.name != "consolidated.csv"
            )
    if not parts:
        logger.warning(
                "No parts to consolidate for "
                f"{provider}/{feed}/{batch_id}"
        )
        return
    out_cols = None
    seen = set()
    out_path = base / "consolidated.csv"
    rows_out = 0
    with out_path.open("w", encoding="utf-8", newline="") as fout:
        writer = None
        for part in parts:
            with part.open("r", encoding="utf-8", newline="") as fin:
                reader = csv.DictReader(fin)
                if not reader.fieldnames:
                    logger.warning(
                            f"Skip empty/invalid part (no header): {part}"
                    )
                    continue

                if out_cols is None:
                    out_cols = list(reader.fieldnames) 
                    writer = csv.DictWriter(fout, fieldnames=out_cols)
                    writer.writeheader()

                for row in reader:
                    key = (row["movie_key"], row["record_hash"])
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow(row)
                    rows_out += 1
    logger.info(
            f"Consolidated {len(parts)} part(s) "
            f"-> {out_path} (rows={rows_out})"
    )


def required_feeds_map(mappings_cfg: dict) -> Dict[str, List[str]]:
    out = {}
    for provider, pcfg in mappings_cfg.get("providers", {}).items():
        req = []
        for fname, fcfg in pcfg.get("feeds", {}).items():
            if fcfg.get("required", True):
                req.append(fname)
        out[provider] = sorted(req)
    return out


def batch_completeness(qualified_plan: List[Dict[str, Any]],
                       mappings_cfg: dict) -> Dict[Tuple[str,str], dict]:
    """Return {(provider,batch_id): {"present": set(feeds), "required": set(...), "complete": bool}}"""
    required = required_feeds_map(mappings_cfg)
    present = defaultdict(set)
    for it in qualified_plan:
        present[(it["provider"], it["batch_id"])].add(it["feed"])

    result = {}
    for (prov, bid), pres in present.items():
        req = set(required.get(prov, []))
        complete = req.issubset(pres)
        result[(prov, bid)] = {"present": pres, "required": req, "complete": complete}
    return result


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
    logger.info("All contracts and configs loaded successfully.")

    batches = discover_batches(paths)
    logger.info(f"Found batches: {batches}")

    plan = build_plan(batches, mappings)

    qualified_plan = precheck_and_schema_gate(plan, mappings)
    logger.info(f"Qualified plan size: {len(qualified_plan)}")

    for q in qualified_plan:
        ingest_domestic_csv(q, mappings)
        ingest_international_csv(q, mappings)
        ingest_financials_csv(q, mappings)

    summary = batch_completeness(qualified_plan, mappings)
    for (prov, bid), info in sorted(summary.items()):
        write_audit(provider=prov, batch_id=bid, level="batch",
                    completeness=sorted(list(info["present"])),
                    required=sorted(list(info["required"])),
                    complete=info["complete"], status="ok")
        if not info["complete"]:
            logger.warning(f"Batch incomplete → skip silver later: {prov}/{bid}. "
                           f"present={sorted(info['present'])}, required={sorted(info['required'])}")

    processed_batches = {
            (i["provider"], i["batch_id"]) for i in qualified_plan
            }

    for provider, batch_id in sorted(processed_batches):
        for feed in ["domestic_csv", "international_csv", "financials_csv"]:
            consolidate_bronze_feed(provider, feed, batch_id)


if __name__ == "__main__":
    main()

