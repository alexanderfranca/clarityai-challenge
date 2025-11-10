from pathlib import Path
from typing import (
        Any,
        Dict,
        List,
        Tuple,
        Optional,
)
import csv
import json
import hashlib
from datetime import (
        datetime,
        timezone,
        )
from .audit import already_processed


def _sha256_file(path: Path) -> str:
    """
    Create a hash id based on the file content.
    It can be improved for a more performatic version.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _csv_header(
        path: Path,
        delimiter: str = ",",
        encoding: str = "utf-8"
        ) -> List[str]:
    """
    Returns the CSV header.
    It tries to infer the CSV format using Sniffer.
    """
    with path.open("r", encoding=encoding, newline="") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        if not delimiter:
            try:
                delimiter = sniffer.sniff(sample).delimiter
            except csv.Error:
                delimiter = ","
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, None)
        if not header:
            raise ValueError("File is empty or missing header")
        return [h.strip() for h in header]


def _required_source_cols(feed_cfg: Dict[str, Any]) -> List[str]:
    """
    Return the columns required for the ingestion.
    """
    return sorted(list(feed_cfg.get("mappings", {}).keys()))


def _csv_options(feed_cfg: Dict[str, Any]) -> Tuple[str, str]:
    """
    The contract can have specific CSV options like encoding of delimiter.
    This function returns the found options to read CSV files.
    """
    opts = feed_cfg.get("csv_options", {})
    return opts.get("delimiter", ","), opts.get("encoding", "utf-8")


def _sample_json_records(path: Path, max_records: int = 50):
    """
    Reads a sample of a file to detect JSON format.
    It's important to avoid loading a big and broken JSON file
    and/or to avoid trying to read non-JSON file.
    """
    with path.open("r", encoding="utf-8") as f:
        first = f.read(2048)
        f.seek(0)
        if first.lstrip().startswith("["):
            for rec in json.load(f)[:max_records]:
                if isinstance(rec, dict):
                    yield rec
            return

        count = 0
        for line in f:
            line = line.strip()
            if not line:
                continue

            rec = json.loads(line)
            if isinstance(rec, dict):
                yield rec
                count += 1
                if count >= max_records:
                    break


def precheck_csv(
        item: Dict[str, Any],
        feed_cfg: Dict[str, Any],
        logger
        ) -> Optional[Dict[str, Any]]:
    """
    Check:
    - CSV sanity (header for example);
    - CSV exists;
    - It was already processed.

    Add metadata about the file in the pipeline.
    Example of added metadata:
    - size in bytes
    - file hash
    - date/time of modification
    """
    file_path = Path(item["file"])
    if not file_path.exists() or not \
            file_path.is_file() or file_path.stat().st_size <= 0:
        logger.error(f"File missing/empty: {file_path}")
        return None

    file_hash = _sha256_file(file_path)
    if already_processed(
            item["provider"],
            item["batch_id"],
            file_path.name,
            file_hash):
        logger.info(
                "Skip (already processed): "
                f"{file_path.name}")
        return None

    delimiter, encoding = _csv_options(feed_cfg)
    try:
        header = _csv_header(file_path, delimiter=delimiter, encoding=encoding)
    except Exception as e:
        logger.error(
                "Cannot read header for "
                f"{file_path.name}: {e}"
        )
        return None

    required = _required_source_cols(feed_cfg)
    missing = [c for c in required if c not in header]
    extra = [c for c in header if c not in required]

    if missing:
        logger.error(
                "Schema gate failed (CSV): "
                f"{file_path.name} "
                f"missing {missing}")
        return None

    if extra:
        logger.warning(
                "Additive columns in "
                f"{file_path.name}: "
                f"{extra} (it's allowed in bronze)"
        )

    # Try to read one row to check if the file is actually ok.
    try:
        with file_path.open("r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            next(reader, None)
    except Exception as e:
        logger.error(
                f"CSV parse sanity failed for "
                f"{file_path.name}: {e}"
        )
        return None

    # TODO: metadata columns inclusion should come from a general function.
    return {
        **item,
        "size_bytes": file_path.stat().st_size,
        "source_mod_time": datetime.fromtimestamp(
                            file_path.stat().st_mtime, tz=timezone.utc
                           ).isoformat(),
        "file_hash": file_hash,
        "header": header,
        "required_cols": required,
        "extra_cols": extra,
    }


def precheck_json(
        item: Dict[str, Any],
        feed_cfg: Dict[str, Any],
        logger
        ) -> Optional[Dict[str, Any]]:
    """
    Check:
    - the sanity of a JSON file;
    - if the file was already processed;

    Add metadata columns like:
    - size in bytes
    - date of modification
    """
    file_path = Path(item["file"])
    if not file_path.exists() or not\
            file_path.is_file() or file_path.stat().st_size <= 0:
        logger.error(
                "File missing/empty: "
                f"{file_path}")
        return None

    file_hash = _sha256_file(file_path)
    if already_processed(
            item["provider"],
            item["batch_id"],
            file_path.name,
            file_hash
            ):
        logger.info(
                "Skip (already processed): "
                f"{file_path.name}")
        return None

    required = set(_required_source_cols(feed_cfg))
    seen_keys = set()

    try:
        for rec in _sample_json_records(file_path, max_records=50):
            seen_keys.update(rec.keys())
    except Exception as e:
        logger.error(
                "Cannot sample JSON for "
                f"{file_path.name}: {e}")
        return None

    missing = sorted(list(required - seen_keys))
    extra = sorted(list(seen_keys - required))

    if missing:
        logger.error(
                "Schema gate failed (JSON): "
                f"{file_path.name} missing {missing}")
        return None

    if extra:
        logger.warning(
                "Additive fields in "
                f"{file_path.name}: {extra} (allowed in bronze)"
        )

    # TODO: metadata columns inclusion should come from a general function.
    return {
        **item,
        "size_bytes": file_path.stat().st_size,
        "source_mod_time": datetime.fromtimestamp(
                            file_path.stat().st_mtime, tz=timezone.utc
                           ).isoformat(),
        "file_hash": file_hash,
        "header": sorted(list(seen_keys)),
        "required_cols": sorted(list(required)),
        "extra_cols": extra,
    }


def qualify_plan(
        plan: List[Dict[str, Any]],
        mappings_cfg: Dict[str, Any],
        logger) -> List[Dict[str, Any]]:
    """
    From the expected plan, make sure the files can be ingested.
    For example checks if the CSV or JSON are acutally valid formats.

    Also generated the new plan with the metadata columns.
    """
    qualified: List[Dict[str, Any]] = []
    for it in plan:
        prov = it["provider"]
        feed = it["feed"]
        feed_cfg = mappings_cfg.get(
                "providers", {}
                ).get(prov, {}).get("feeds", {}).get(feed)

        if not feed_cfg:
            logger.error(
                    f"Missing feed config: {prov}.{feed}"
            )
            continue

        fmt = feed_cfg.get("input_format", "csv").lower()
        if fmt == "csv":
            enriched = precheck_csv(it, feed_cfg, logger)
        elif fmt == "json":
            enriched = precheck_json(it, feed_cfg, logger)
        else:
            logger.error(
                    "Unsupported input_format "
                    f"'{fmt}' for {prov}.{feed}")
            enriched = None

        if enriched:
            qualified.append(enriched)

    if not qualified:
        logger.error("No files passed precheck/schema gate.")
    else:
        logger.info(
                "Precheck complete: "
                f"{len(qualified)}/{len(plan)} file(s) qualified."
        )
    return qualified
