from src.core.audit import (
        write_audit,
        already_processed,
)


def test_audit_write_and_check(temp_project):
    write_audit(
            provider="p",
            batch_id="b1",
            source_file="f.csv",
            file_hash="h1"
            )
    write_audit(
            provider="p",
            batch_id="b2",
            source_file="f.csv",
            file_hash="h2"
            )
    assert already_processed("p", "b1", "f.csv", "h1") is True
    assert already_processed("p", "b1", "f.csv", "other") is False
