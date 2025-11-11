import pandas as pd
from src.core.gold import build_gold


def test_build_gold_from_silver(temp_project, monkeypatch):
    silver_dir = temp_project["silver"]
    df = pd.DataFrame(
        [
            {
                "movie_key": "k1",
                "title": "A",
                "year": 2001,
                "domestic_box_office_gross": 10,
                "international_box_office_gross": 5,
                "production_budget_usd": 3,
                "marketing_spend_usd": 2,
                "critic_score_percentage": 90,
                "audience_score_100": 95,
            }
        ]
    )
    sf = silver_dir / "movie_metrics_20250101T000000Z.csv"
    df.to_csv(sf, index=False)

    import src.core.gold as gold

    gold.SILVER_DIR = silver_dir
    gold.GOLD_DIR = temp_project["gold"]
    gold.AUDIT_LEDGER = temp_project["audit"]

    class L:
        def info(self, *a, **k):
            pass

        def warning(self, *a, **k):
            pass

        def error(self, *a, **k):
            pass

    logger = L()

    out = build_gold(logger)
    assert out and out.exists()
