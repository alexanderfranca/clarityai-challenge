from pathlib import Path
from datetime import datetime, timezone
import json


BASE_DIR = Path(__file__).resolve().parents[2]
AUDIT_LEDGER = BASE_DIR / "audit" / "ledger.jsonl"
AUDIT_LEDGER.parent.mkdir(parents=True, exist_ok=True)


def already_processed(
        provider: str,
        batch_id: str,
        source_file: str,
        file_hash: str) -> bool:

    if not AUDIT_LEDGER.exists():
        return False

    key = (provider, batch_id, source_file, file_hash)
    with AUDIT_LEDGER.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                if (
                    rec.get("provider"),
                    rec.get("batch_id"),
                    rec.get("source_file"),
                    rec.get("file_hash")
                   ) == key:
                    return True
            except Exception:
                continue
    return False


def write_audit(**kwargs):
    rec = dict(kwargs)
    rec.setdefault("ts", datetime.now(timezone.utc).isoformat())
    with AUDIT_LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
