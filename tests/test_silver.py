import pandas as pd
from src.core.silver import build_silver_movie_metrics
from src.core.audit import write_audit


def test_build_silver_movie_metrics(temp_project):
    for prov in ["boxofficemetrics", "criticagg", "audiencepulse"]:
        write_audit(provider=prov, batch_id="b1", level="batch", complete=True, status="ok")

    bd = temp_project["bronze"] / "boxofficemetrics" / "domestic_csv" / "b1"
    bd.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"movie_key": "k1", "title": "A", "year": 2001, "domestic_box_office_gross": 10}]).to_csv(bd/"consolidated.csv", index=False)

    bi = temp_project["bronze"] / "boxofficemetrics" / "international_csv" / "b1"
    bi.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"movie_key": "k1", "title": "A", "year": 2001, "international_box_office_gross": 5}]).to_csv(bi/"consolidated.csv", index=False)

    bf = temp_project["bronze"] / "boxofficemetrics" / "financials_csv" / "b1"
    bf.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"movie_key": "k1", "title": "A", "year": 2001, "production_budget_usd": 3, "marketing_spend_usd": 2}]).to_csv(bf/"consolidated.csv", index=False)

    cr = temp_project["bronze"] / "criticagg" / "reviews_csv" / "b1"
    cr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"movie_key": "k1", "title": "A", "year": 2001, "critic_score_percentage": 90, "top_critic_score": 8.5, "critic_votes": 100}]).to_csv(cr/"consolidated.csv", index=False)

    ap = temp_project["bronze"] / "audiencepulse" / "ratings_json" / "b1"
    ap.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"movie_key": "k1", "title": "A", "year": 2001, "audience_average_score": 9.0, "audience_votes": 50}]).to_csv(ap/"consolidated.csv", index=False)
    contracts = {
        "optional_columns_order": [],
        "entities": {
            "audiencepulse_ratings_stage": {
                "columns": [
                    {"name": "provider"}, {"name": "feed"}, {"name": "batch_id"}, {"name": "source_file"},
                    {"name": "source_mod_time"}, {"name": "file_hash"}, {"name": "ingest_ts"}, {"name": "schema_version"},
                    {"name": "record_hash"},
                    {"name": "movie_key"}, {"name": "title"}, {"name": "year"},
                    {"name": "audience_average_score"}, {"name": "total_audience_ratings"}
                ]
            }
        }
    }
    curation = {
        "entities": {
            "movie_metrics": {
                "join_key": "movie_key",
                "sources": {
                    "boxofficemetrics": {"feeds": ["domestic_csv", "international_csv", "financials_csv"]},
                    "criticagg": {"feeds": ["reviews_csv"]},
                    "audiencepulse": {"feeds": ["ratings_json"]}
                },
                "quality_checks": {"not_null": ["movie_key", "title", "year"], "non_negative": []},
                "output_path": "data/silver/movie_metrics/"
            }
        }
    }

    class L:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    logger = L()

    out = build_silver_movie_metrics(curation, contracts, logger)
    assert out and out.exists()
    df = pd.read_csv(out)
    assert len(df) == 1
    assert df.loc[0, "domestic_box_office_gross"] == 10
    assert df.loc[0, "international_box_office_gross"] == 5
