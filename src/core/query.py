from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Dict, Tuple
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_PATTERNS = [
    "movie_metrics_final_*.csv",
    "movie_metrics_*.csv",
    "*.csv",
]


@dataclass(frozen=True)
class MovieKey:
    movie_key: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None


def _find_latest_gold_file() -> Path:
    candidates: list[Path] = []
    for pattern in GOLD_PATTERNS:
        candidates.extend(sorted(GOLD_DIR.glob(pattern)))
    if not candidates:
        raise FileNotFoundError(f"No gold CSV found in {GOLD_DIR}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def load_gold_df(path: Optional[Union[str, Path]] = None) -> pd.DataFrame:
    csv_path = Path(path) if path else _find_latest_gold_file()
    df = pd.read_csv(csv_path)
    if "title" in df.columns:
        df["title"] = df["title"].astype(str).str.strip().str.lower()
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def _has_movie_key(df: pd.DataFrame) -> bool:
    return "movie_key" in df.columns


def as_dict_by_key(
        df: pd.DataFrame) -> Dict[Union[str, Tuple[str, int]], dict]:
    result: Dict[Union[str, Tuple[str, int]], dict] = {}
    if _has_movie_key(df):
        for _, row in df.iterrows():
            result[str(row["movie_key"])] = row.to_dict()
    else:
        for _, row in df.iterrows():
            t = str(row.get("title", row.get("title", ""))).strip().lower()
            y = row.get("year")
            result[(t, int(y) if pd.notna(y) else None)] = row.to_dict()
    return result


def find_by_title(df: pd.DataFrame, title_substr: str, year: Optional[int] = None) -> pd.DataFrame:
    mask = df["title"].str.contains(title_substr.strip().lower(), na=False)
    if year is not None and "year" in df.columns:
        mask &= (df["year"] == int(year))
    return df.loc[mask].copy()


def lookup(df: pd.DataFrame, key: MovieKey) -> pd.DataFrame:
    if key.movie_key and _has_movie_key(df):
        return df.loc[df["movie_key"].astype(str) == str(key.movie_key)].copy()

    if key.title is None:
        raise ValueError("Provide either movie_key or (title AND year)")

    q = df["title"] == key.title.strip().lower()
    if key.year is not None and "year" in df.columns:
        q &= df["year"] == int(key.year)
    return df.loc[q].copy()


def head(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    return df.head(n).copy()

