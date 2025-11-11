from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = BASE_DIR / "data" / "gold"


def load_gold_as_dict() -> dict[str, dict]:
    latest = sorted(GOLD_DIR.glob("movie_metrics_final_*.csv"))[-1]
    df = pd.read_csv(latest)

    key_col = "movie_key" if "movie_key" in df.columns else None
    if key_col:
        return {
            str(r[key_col]): r._asdict() if hasattr(r, "_asdict") else r.to_dict()
            for _, r in df.iterrows()
        }

    return {
        (str(r["title"]).strip(), int(r["year"])): r.to_dict() for _, r in df.iterrows()
    }
