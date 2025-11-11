from __future__ import annotations
from functools import reduce
import json
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)
from datetime import datetime, timezone
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_ROOT = BASE_DIR / "data" / "bronze"
SILVER_ROOT = BASE_DIR / "data" / "silver"
AUDIT_LEDGER = BASE_DIR / "audit" / "ledger.jsonl"


def _latest_complete_batch(provider: str) -> Optional[str]:
    """
    Return the most recent batch_id for a provider where the batch audit
    recorded complete=true. If none, return None.
    """
    if not AUDIT_LEDGER.exists():
        return None

    latest: Optional[Tuple[str, datetime]] = None
    with AUDIT_LEDGER.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if (
                rec.get("level") == "batch"
                and rec.get("provider") == provider
                and rec.get("complete") is True
            ):
                ts_str = rec.get("ts")
                ts = None
                try:
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception:
                    ts = None

                key_time = ts or datetime.min.replace(tzinfo=timezone.utc)

                if latest is None or key_time > latest[1]:
                    latest = (rec.get("batch_id"), key_time)

    return latest[0] if latest else None


def _consolidated_path(provider: str, feed: str, batch_id: str) -> Path:
    """
    Returns the consolidated file from bronze.
    """
    return BRONZE_ROOT / provider / feed / batch_id / "consolidated.csv"


def _resolve_inputs(curation_cfg: dict, logger) -> Dict[str, Dict[str, Path]]:
    """
    For each provider listed in curation, pick the latest complete batch and
    return a mapping {provider: {feed: consolidated.csv path}}
    Missing files are logged and omitted.
    """
    providers = curation_cfg["entities"]["movie_metrics"]["sources"].keys()
    resolved: Dict[str, Dict[str, Path]] = {}

    for provider in providers:
        batch_id = _latest_complete_batch(provider)
        if not batch_id:
            logger.warning(
                f"[silver] No complete batch for provider={provider}; "
                "skipping provider."
            )
            continue

        feeds = curation_cfg["entities"]["movie_metrics"]["sources"][provider]["feeds"]
        feed_paths: Dict[str, Path] = {}
        for feed in feeds:
            p = _consolidated_path(provider, feed, batch_id)
            if p.exists():
                feed_paths[feed] = p
            else:
                logger.warning(
                    f"[silver] Missing consolidated for {provider}/{feed}/{batch_id} -> {p}"
                )

        if feed_paths:
            resolved[provider] = feed_paths

        # Example of feed_paths:
        # {
        # 'domestic_csv': PosixPath('.../bronze/boxofficemetrics/domestic_csv/2025-11-10T020015Z/consolidated.csv'),
        # 'financials_csv': PosixPath('.../bronze/boxofficemetrics/financials_csv/2025-11-10T020015Z/consolidated.csv'),
        # 'international_csv': PosixPath('.../bronze/boxofficemetrics/international_csv/2025-11-10T020015Z/consolidated.csv')
        # }

    return resolved


def _read_df(path: Path, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Read a CSV file and returns a dataframe with a specific set of columns.
    """
    df = pd.read_csv(path)
    if usecols:
        cols = [c for c in usecols if c in df.columns]
        df = df[cols]
    return df


def _nonneg(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df = df[df[col].isna() | (df[col] >= 0)]
    return df


def _load_consolidated_into_df(path: str, lineage_columns: list) -> pd.DataFrame:
    """
    Read CSV file into a Pandas DataFrame.
    """
    df = pd.read_csv(path)

    if "ingest_ts" in df.columns:
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True, errors="coerce")
    else:
        df["ingest_ts"] = pd.NaT

    df = df.drop(columns=lineage_columns, errors="ignore")
    return df


def build_silver_movie_metrics(
    curation_cfg: dict, contracts: dict, logger
) -> Optional[Path]:
    """
    Build the unified 'movie_metrics' silver table by joining bronze consolidated
    outputs across providers on movie_key, applying precedence for title/year,
    light QC, and writing a single CSV.
    """
    entity = curation_cfg["entities"]["movie_metrics"]
    join_key = entity["join_key"]
    out_dir = BASE_DIR / entity["output_path"]
    out_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------------
    # resolved = all the generated consolidated.csv full path existing in
    #            bronze from all providers from the last complete batch run.
    #            Basically... the whole recent content of bronze.
    # ----------------------------------------------------------------------
    # Example of 'resolved' variable value:
    #
    # {
    #  'audiencepulse':
    #                      {
    #                        'ratings_json': PosixPath('.../bronze/audiencepulse/ratings_json/2025-11-10T020015Z/consolidated.csv')
    #                      },
    #  'boxofficemetrics': {
    #                       'domestic_csv': PosixPath('.../bronze/boxofficemetrics/domestic_csv/2025-11-10T020015Z/consolidated.csv'),
    #                       'financials_csv': PosixPath('.../bronze/boxofficemetrics/financials_csv/2025-11-10T020015Z/consolidated.csv'),
    #                       'international_csv': PosixPath('.../bronze/boxofficemetrics/international_csv/2025-11-10T020015Z/consolidated.csv')
    #                      },
    # 'criticagg':         {
    #                       'reviews_csv': PosixPath('.../bronze/criticagg/reviews_csv/2025-11-10T010015Z/consolidated.csv')}
    #                      }
    # ----------------------------------------------------------------------
    resolved = _resolve_inputs(curation_cfg, logger)
    if not resolved:
        logger.error("[silver] No inputs resolved. Aborting silver build.")
        return None

    lineage_columns_raw = contracts.get("lineage_columns", {})

    # Columns to ignore, but keeping the ingest_ts
    lineage_columns = [
        item.get("name", {})
        for item in lineage_columns_raw
        if item.get("name", {}) != "ingest_ts"
    ]

    all_raw_dfs = {}
    for provider, feeds in resolved.items():
        for feed, path in feeds.items():
            df = _load_consolidated_into_df(str(path), lineage_columns)
            df = df.sort_values(["ingest_ts", join_key]).drop_duplicates(
                subset=[join_key], keep="last"
            )

            all_raw_dfs[(provider, feed)] = df

    # TODO: To apply a better and more clear method/algorithm
    merged_dfs = [
        d.drop_duplicates(subset=[join_key], keep="last").set_index(join_key)
        for (provider, feed), d in all_raw_dfs.items()
    ]
    # This collapses the merged_dfs.
    base = reduce(lambda a, b: a.combine_first(b), merged_dfs).reset_index()
    base = base.drop_duplicates(subset=[join_key])

    quality_checks = entity.get("quality_checks", {})
    not_null = quality_checks.get("not_null", ["movie_key", "title", "year"])
    for col in not_null:
        if col in base.columns:
            base = base[base[col].notna()]

    nonneg_cols = quality_checks.get("non_negative", [])
    for col in nonneg_cols:
        base = _nonneg(base, col)

    # Set column order (optional in contracts but nice to have)
    desired_order = []
    if contracts["optional_columns_order"]:
        desired_order = contracts["optional_columns_order"]
    ordered = [c for c in desired_order if c in base.columns]
    remaining = [c for c in base.columns if c not in ordered]
    base = base[ordered + remaining]

    # Write + audit
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = SILVER_ROOT / "movie_metrics" / f"movie_metrics_{ts}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(out_path, index=False)

    # Silver audit record (append to ledger)
    if AUDIT_LEDGER.exists() or True:
        rec = {
            "level": "silver",
            "entity": "movie_metrics",
            "rows_out": int(len(base)),
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": "ok",
            "path": str(out_path),
            "inputs": {
                prov: {feed: str(p) for feed, p in feeds.items()}
                for prov, feeds in resolved.items()
            },
        }
        with AUDIT_LEDGER.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return out_path
