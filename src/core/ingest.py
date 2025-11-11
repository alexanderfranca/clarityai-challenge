from pathlib import Path
from datetime import (
        datetime,
        timezone,
)
from typing import (
        Dict,
        Any,
        List,
        Iterable,
        Iterator,
)
import csv
import hashlib
import json
from .audit import write_audit


BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_ROOT = BASE_DIR / "data" / "bronze"
META_COLS = {
    "provider",
    "feed",
    "batch_id",
    "source_file",
    "source_mod_time",
    "file_hash",
    "ingest_ts",
    "schema_version",
    "record_hash",
}


def _cast_value(v, typ: str):
    """
    Try to cast the value (v) into one of Python types.
    """
    if v is None or v == "":
        return None

    t = (typ or "string").lower()

    if t in ("string", "str"):
        return _collapse_ws(str(v))
    if t in ("int", "integer"):
        try:
            n = int(v)
            return n
        except Exception:
            return None
    if t in ("float", "double", "number"):
        try:
            return float(v)
        except Exception:
            return None
    return v


def _collapse_ws(s: str) -> str:
    """
    Collapse white spaces into a single one.
    """
    return " ".join(str(s).strip().split())


def _derive_movie_key_v1(title: str, year: int) -> str:
    """
    Create a key based on title and year.
    It's basic and naive only for the purpose of the challenge.
    """
    norm = f"{_collapse_ws(title).lower()}|{year}"
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def _nonneg_or_none(x):
    """
    Check if it's a number and not negative.
    Cast to None if it fails.
    """
    try:
        v = int(x)
        return v if v >= 0 else None
    except Exception:
        return None


def _apply_normalize(v, normalizers_to_execute: Iterable[str]):
    """
    Normalize the values, for example:
    - trim strings
    - collapse white spaces
    - convert to lower case
    """
    if v is None:
        return None

    out = str(v)
    for op in normalizers_to_execute or []:
        normalizer = op.lower()
        if normalizer == "strip":
            out = out.strip()
        elif normalizer == "collapse_ws":
            out = " ".join(out.split())
        elif normalizer == "lower":
            out = out.lower()
    return out


# TODO: for CSV with big amount of columns this can kill the performance.
def _hash_payload(cols: List[str], row: dict) -> str:
    """
    Create a hash for a line.
    """
    # TODO: THIS WILL KILL PERFORMANCE for big amount of columns.
    payload = "|".join(
            "" if row.get(c) is None else str(row.get(c)) for c in cols
            )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# TODO: split into domain actions. This function is TOO BIG.
def ingest_csv(
        item: dict,
        mappings_cfg: dict,
        contracts_cfg: dict,
        logger) -> None:
    """
    Generic bronze CSV ingester driven by mappings.yaml and contracts.yaml.

    - Uses feed.mapping to rename/cast/normalize columns into target fields.
    - Uses contracts.entities[target_entity].columns for output column set.
    - Derives movie_key via 'derives' or (title, year) heuristic.
    - record_hash: uses mapping.record_identity.columns if present,
        else all business columns.
    - Dedup within-file by (movie_key, record_hash).
    """
    provider = item["provider"]
    batch_id = item["batch_id"]
    feed = item["feed"]
    feed_cfg = mappings_cfg["providers"][provider]["feeds"][feed]
    if feed_cfg.get("input_format", "csv").lower() != "csv":
        logger.info("Not a specified CSV format")
        return

    file_path = Path(item["file"])
    csv_opts = feed_cfg.get("csv_options", {})
    delimiter = csv_opts.get("delimiter", ",")
    encoding = csv_opts.get("encoding", "utf-8")
    schema_version = mappings_cfg["providers"][provider].get(
            "schema_version", 1
            )

    target_entity = feed_cfg.get("target_entity")
    ent = contracts_cfg.get("entities", {}).get(target_entity)
    if not ent:
        logger.error(f"Contract missing for entity: {target_entity}")
        return

    contract_cols: List[str] = [c["name"] for c in ent.get("columns", [])]
    business_cols = [c for c in contract_cols if c not in META_COLS]
    mappings = feed_cfg.get("mappings", {})

    # Record identity columns (business)
    # if specified; else default to ALL business cols
    rec_id_cfg = feed_cfg.get("record_identity", {})
    rec_id_cols = rec_id_cfg.get("columns") or business_cols

    # Prepare IO
    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{item['file_hash'][:12]}.csv"

    rows_in = 0
    rows_out = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()
    seen = set()

    with file_path.open("r", encoding=encoding, newline="") as fin, \
            out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin, delimiter=delimiter)
        writer = csv.DictWriter(fout, fieldnames=contract_cols)
        writer.writeheader()

        for raw in reader:
            rows_in += 1

            # Apply mappings (rename/cast/normalize)
            mapped: Dict[str, Any] = {}
            for src, spec in mappings.items():
                target_name = spec["to"]
                typ = spec.get("type", "string")
                norm_ops = spec.get("normalize", [])
                clamp = spec.get("clamp")
                val = raw.get(src)
                val = _cast_value(val, typ)
                val = _apply_normalize(val, norm_ops)

                # clamp for non-negative ints
                if clamp and isinstance(val, (int, float)):
                    lo, hi = clamp[0], clamp[1] if len(clamp) > 1 else None
                    if lo is not None and isinstance(lo, (int, float)) and val < lo:
                        val = None
                    if hi is not None and isinstance(hi, (int, float)) and val > hi:
                        val = None

                mapped[target_name] = val

            # Movie key, explicit (from "derives") or heuristic
            derives = feed_cfg.get("derives", {})
            if "movie_key" in derives:
                # TODO: to be implemented
                pass
            if "movie_key" not in mapped:
                if "title" in mapped and "year" in mapped and mapped["year"] is not None:
                    mapped["movie_key"] = _derive_movie_key_v1(mapped["title"], mapped["year"])

            # Record hash based on the columns
            record_hash_cols = [c for c in rec_id_cols if c in mapped]
            if not record_hash_cols:
                record_hash_cols = [c for c in business_cols if c in mapped]
            record_hash = _hash_payload(sorted(record_hash_cols), mapped)

            # Deduplication based on key
            dedupe_key = (mapped.get("movie_key"), record_hash)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            # Build output row
            out_row = {}
            # metadata
            out_row.update({
                "provider": provider,
                "feed": feed,
                "batch_id": batch_id,
                "source_file": file_path.name,
                "source_mod_time": item["source_mod_time"],
                "file_hash": item["file_hash"],
                "ingest_ts": ingest_ts,
                "schema_version": schema_version,
                "record_hash": record_hash,
            })
            # business: columns defined in contract
            for c in business_cols:
                out_row[c] = mapped.get(c)

            # if mapping produced fields that are not in contract
            extra_fields = [k for k in mapped.keys() if k not in business_cols]
            if extra_fields:
                logger.debug(
                        f"{provider}.{feed}: "
                        f"ignoring non-contract fields {extra_fields}"
                )

            writer.writerow(out_row)
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


def _iter_json_records(path: Path) -> Iterator[dict]:
    """
    This helper it allow JSONL and single JSON files.
    """
    with path.open("r", encoding="utf-8") as f:
        head = f.read(2048)
        f.seek(0)
        if head.lstrip().startswith("["):
            data = json.load(f)
            if not isinstance(data, list):
                return
            for obj in data:
                if isinstance(obj, dict):
                    yield obj
            return
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj


def ingest_json(item: dict,
        mappings_cfg: dict,
        contracts_cfg: dict,
        logger) -> None:
    """
    Bronze JSON ingester (mapping + contract driven).
    - Reads JSON Lines or a single JSON array.
    - Applies mappings (rename/cast/normalize) from mappings.yaml.
    - Aligns output columns & order to contracts.yaml for target_entity.
    - Derives movie_key (or title+year fallback).
    - record_hash from record_identity.columns (or all business columns).
    - Dedup within-file by (movie_key, record_hash).
    - Stamps bronze metadata and writes CSV into bronze.
    """
    provider = item["provider"]
    batch_id = item["batch_id"]
    feed = item["feed"]
    feed_cfg = mappings_cfg["providers"][provider]["feeds"][feed]
    if (feed_cfg.get("input_format") or "csv").lower() != "json":
        return

    target_entity = feed_cfg.get("target_entity")
    ent = contracts_cfg.get("entities", {}).get(target_entity)
    if not ent:
        logger.error(
                f"Contract missing for entity: {target_entity}"
        )
        return

    contract_cols: List[str] = [c["name"] for c in ent.get("columns", [])]
    business_cols = [c for c in contract_cols if c not in META_COLS]

    mappings = feed_cfg.get("mappings", {})
    rec_id_cfg = feed_cfg.get("record_identity", {})
    rec_id_cols = rec_id_cfg.get("columns") or business_cols

    schema_version = mappings_cfg["providers"][provider].get(
            "schema_version", 1
            )
    file_path = Path(item["file"])

    out_dir = BRONZE_ROOT / provider / feed / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{item['file_hash'][:12]}.csv"

    rows_in = 0
    rows_out = 0
    ingest_ts = datetime.now(timezone.utc).isoformat()
    seen = set()

    with out_path.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=contract_cols)
        writer.writeheader()

        for rec in _iter_json_records(file_path):
            rows_in += 1
            # map/normalize/cast per mappings
            mapped: Dict[str, Any] = {}
            for src, spec in mappings.items():
                target_name = spec["to"]
                typ = spec.get("type", "string")
                norm_ops = spec.get("normalize", [])
                clamp = spec.get("clamp")  # e.g. [0, null]
                val = rec.get(src)
                val = _cast_value(val, typ)
                val = _apply_normalize(val, norm_ops)

                if clamp and isinstance(val, (int, float)):
                    lo = clamp[0] if len(clamp) > 0 else None
                    hi = clamp[1] if len(clamp) > 1 else None
                    if lo is not None and isinstance(
                            lo, (int, float)
                            ) and val < lo:
                        val = None
                    if hi is not None and isinstance(
                            hi, (int, float)
                            ) and val > hi:
                        val = None

                mapped[target_name] = val

            # derive movie_key (or title+year fallback)
            derives = feed_cfg.get("derives", {})
            if "movie_key" in derives:
                # TODO: to be implemented
                pass
            if "movie_key" not in mapped:
                if "title" in mapped and "year" in mapped and mapped["year"] is not None:
                    try:
                        mapped["movie_key"] = _derive_movie_key_v1(mapped["title"], int(mapped["year"]))
                    except Exception:
                        mapped["movie_key"] = None

            if "movie_key" in business_cols and not mapped.get("movie_key"):
                continue

            # Record hash based on the columns
            record_hash_cols = [c for c in rec_id_cols if c in mapped]
            if not record_hash_cols:
                record_hash_cols = [c for c in business_cols if c in mapped]
            record_hash = _hash_payload(sorted(record_hash_cols), mapped)

            # Deduplication baseed on key
            dedupe_key = (mapped.get("movie_key"), record_hash)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            # Build output row 
            out_row = {
                "provider": provider,
                "feed": feed,
                "batch_id": batch_id,
                "source_file": file_path.name,
                "source_mod_time": item["source_mod_time"],
                "file_hash": item["file_hash"],
                "ingest_ts": ingest_ts,
                "schema_version": schema_version,
                "record_hash": record_hash,
            }
            # business: columns defined in contract
            for c in business_cols:
                out_row[c] = mapped.get(c)

            writer.writerow(out_row)
            rows_out += 1

    write_audit(
        provider=provider, batch_id=batch_id, feed=feed,
        source_file=file_path.name, file_hash=item["file_hash"],
        rows_in=rows_in, rows_out=rows_out, status="ok"
    )
    logger.info(
            f"Wrote bronze(JSON): {out_path} "
            f"(rows_in={rows_in}, rows_out={rows_out})"
    )


def consolidate_bronze_feed(provider: str, feed: str, batch_id: str, logger):
    """
    Generate a single consolidated CSV file based on all the files found in
    the batch.
    """
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
                    logger.warning(f"Skip invalid part (no header): {part}")
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



