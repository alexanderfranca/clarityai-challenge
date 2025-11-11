import sys
from pathlib import Path
import json
import csv
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def write_yaml(path: Path, data: dict):
    import yaml
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def write_csv(path: Path, rows, header=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        if header is None and rows:
            header = list(rows[0].keys())
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


@pytest.fixture
def temp_project(tmp_path, monkeypatch):
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "incoming").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "bronze").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "silver" / "movie_metrics").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "gold").mkdir(parents=True, exist_ok=True)
    (tmp_path / "audit").mkdir(parents=True, exist_ok=True)


    from src.core import (
            loader,
            audit,
            planner,
            ingest,
    )
    import src.core.silver as silver

    monkeypatch.setattr(loader, "CONFIG_DIR", tmp_path / "configs", raising=False)
    monkeypatch.setattr(audit, "AUDIT_LEDGER", tmp_path / "audit" / "ledger.jsonl", raising=False)
    monkeypatch.setattr(planner, "INCOMING_DIR", tmp_path / "data" / "incoming", raising=False)
    monkeypatch.setattr(ingest, "BRONZE_ROOT", tmp_path / "data" / "bronze", raising=False)
    monkeypatch.setattr(silver, "BRONZE_ROOT", tmp_path / "data" / "bronze", raising=False)
    monkeypatch.setattr(silver, "SILVER_ROOT", tmp_path / "data" / "silver", raising=False)
    monkeypatch.setattr(silver, "AUDIT_LEDGER", tmp_path / "audit" / "ledger.jsonl", raising=False)

    return {
        "root": tmp_path,
        "configs": tmp_path / "configs",
        "incoming": tmp_path / "data" / "incoming",
        "bronze": tmp_path / "data" / "bronze",
        "silver": tmp_path / "data" / "silver" / "movie_metrics",
        "gold": tmp_path / "data" / "gold",
        "audit": tmp_path / "audit" / "ledger.jsonl",
    }
