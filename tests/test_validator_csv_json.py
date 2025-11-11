from src.core.validator import qualify_plan
from src.core.audit import write_audit


def test_qualify_csv_and_skip_idempotent(temp_project):
    incoming = temp_project["incoming"] / "boxofficemetrics" / "b1"
    incoming.mkdir(parents=True, exist_ok=True)
    (incoming / "_READY").write_text("")
    csvp = incoming / "provider3_domestic.csv"
    csvp.write_text("film_name,year_of_release,box_office_gross_usd\nA,2001,10\n", encoding="utf-8")

    mappings = {
        "providers": {
            "boxofficemetrics": {
                "feeds": {
                    "domestic_csv": {
                        "filename_selector": "provider3_domestic*.csv",
                        "input_format": "csv",
                        "csv_options": {"delimiter": ",", "encoding": "utf-8"},
                        "mappings": {
                            "film_name": {"to": "title", "type": "string"},
                            "year_of_release": {"to": "year", "type": "int"},
                            "box_office_gross_usd": {"to": "domestic_box_office_gross", "type": "int"}
                        },
                        "target_entity": "boxofficemetrics_domestic_stage"
                    }
                }
            }
        }
    }

    plan = [{
        "provider": "boxofficemetrics",
        "batch_id": "b1",
        "file": str(csvp),
        "feed": "domestic_csv",
        "target_entity": "boxofficemetrics_domestic_stage",
    }]

    class L:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    logger = L()

    qualified = qualify_plan(plan, mappings, logger)
    assert len(qualified) == 1

    fh = qualified[0]["file_hash"]
    write_audit(provider="boxofficemetrics", batch_id="b1", source_file=csvp.name, file_hash=fh, status="ok")
    qualified2 = qualify_plan(plan, mappings, logger)
    assert len(qualified2) == 0


def test_qualify_json(temp_project):
    incoming = temp_project["incoming"] / "audiencepulse" / "b1"
    incoming.mkdir(parents=True, exist_ok=True)
    (incoming / "_READY").write_text("")
    jp = incoming / "provider2.jsonl"
    jp.write_text('{"title":"Inception","year":2010,"audience_average_score":9.1,"total_audience_ratings":100}\n', encoding="utf-8")

    mappings = {
        "providers": {
            "audiencepulse": {
                "feeds": {
                    "ratings_json": {
                        "filename_selector": "provider2*.json*",
                        "input_format": "json",
                        "mappings": {
                            "title": {"to": "title", "type": "string"},
                            "year": {"to": "year", "type": "int"},
                            "audience_average_score": {"to": "audience_average_score", "type": "float"},
                            "total_audience_ratings": {"to": "total_audience_ratings", "type": "int"}
                        },
                        "target_entity": "audiencepulse_ratings_stage"
                    }
                }
            }
        }
    }

    plan = [{
        "provider": "audiencepulse",
        "batch_id": "b1",
        "file": str(jp),
        "feed": "ratings_json",
        "target_entity": "audiencepulse_ratings_stage",
    }]

    class L:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    logger = L()

    qualified = qualify_plan(plan, mappings, logger)
    assert len(qualified) == 1
    assert qualified[0]["file_hash"]
