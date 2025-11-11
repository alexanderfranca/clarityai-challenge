from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
SILVER_DIR = BASE_DIR / "data" / "silver" / "movie_metrics"
GOLD_DIR = BASE_DIR / "data" / "gold"
AUDIT_LEDGER = BASE_DIR / "audit" / "ledger.jsonl"


def _latest_silver_file() -> Path | None:
    files = sorted(SILVER_DIR.glob("movie_metrics_*.csv"))
    return files[-1] if files else None


def build_gold(logger) -> Path | None:
    src = _latest_silver_file()
    if not src:
        logger.error("[gold] No silver file found. Aborting.")
        return None

    df = pd.read_csv(src)

    # Fill missing numeric values with 0 where appropriate
    num_cols = [c for c in df.columns if df[c].dtype.kind in "if"]
    df[num_cols] = df[num_cols].fillna(0)

    # Derived KPIs
    df["total_box_office_gross_usd"] = (
        df["domestic_box_office_gross"] + df["international_box_office_gross"]
    )

    df = df.drop_duplicates(subset=["movie_key"], keep="first").reset_index(drop=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = GOLD_DIR / f"movie_metrics_final_{ts}.csv"
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    # audit entry
    rec = {
        "level": "gold",
        "entity": "movie_metrics_final",
        "rows_out": int(len(df)),
        "ts": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "input": str(src),
        "path": str(out_path),
    }
    with AUDIT_LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")

    logger.info(f"[gold] Final dataset written: {out_path}")
    return out_path
