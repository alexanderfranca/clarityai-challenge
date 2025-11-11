from src.core.planner import discover_batches, build_plan


def test_discover_and_plan(temp_project):
    incoming = temp_project["incoming"]
    prov = incoming / "boxofficemetrics" / "2025-11-10T020015Z"
    prov.mkdir(parents=True, exist_ok=True)
    (prov / "_READY").write_text("", encoding="utf-8")
    (prov / "provider3_domestic.csv").write_text(
        "film_name,year_of_release,box_office_gross_usd\nA,2001,10\n", encoding="utf-8"
    )

    paths = {
        "providers": {
            "boxofficemetrics": {
                "incoming_dir": str(incoming / "boxofficemetrics"),
                "readiness": {"marker_file": "_READY", "quarantine_seconds": 0},
            }
        }
    }
    mappings = {
        "providers": {
            "boxofficemetrics": {
                "feeds": {
                    "domestic_csv": {
                        "filename_selector": "provider3_domestic*.csv",
                        "target_entity": "boxofficemetrics_domestic_stage",
                    }
                }
            }
        }
    }

    class L:
        def info(self, *a, **k):
            pass

        def warning(self, *a, **k):
            pass

        def error(self, *a, **k):
            pass

    logger = L()

    batches = discover_batches(paths, logger)
    assert "boxofficemetrics" in batches
    plan = build_plan(batches, mappings, logger)
    assert len(plan) == 1
    assert plan[0]["feed"] == "domestic_csv"
