from src.core.ingest import ingest_csv, ingest_json, consolidate_bronze_feed


def test_ingest_csv_generic_and_consolidate(temp_project):
    contracts = {
        "entities": {
            "boxofficemetrics_domestic_stage": {
                "columns": [
                    {"name": "provider"},
                    {"name": "feed"},
                    {"name": "batch_id"},
                    {"name": "source_file"},
                    {"name": "source_mod_time"},
                    {"name": "file_hash"},
                    {"name": "ingest_ts"},
                    {"name": "schema_version"},
                    {"name": "record_hash"},
                    {"name": "movie_key"},
                    {"name": "title"},
                    {"name": "year"},
                    {"name": "domestic_box_office_gross"},
                ]
            }
        }
    }
    mappings = {
        "providers": {
            "boxofficemetrics": {
                "schema_version": 1,
                "feeds": {
                    "domestic_csv": {
                        "input_format": "csv",
                        "csv_options": {"delimiter": ",", "encoding": "utf-8"},
                        "mappings": {
                            "film_name": {"to": "title", "type": "string"},
                            "year_of_release": {"to": "year", "type": "int"},
                            "box_office_gross_usd": {
                                "to": "domestic_box_office_gross",
                                "type": "int",
                            },
                        },
                        "record_identity": {
                            "columns": ["title", "year", "domestic_box_office_gross"]
                        },
                        "target_entity": "boxofficemetrics_domestic_stage",
                    }
                },
            }
        }
    }
    incoming = temp_project["incoming"] / "boxofficemetrics" / "b1"
    incoming.mkdir(parents=True, exist_ok=True)
    p = incoming / "provider3_domestic.csv"
    p.write_text(
        "film_name,year_of_release,box_office_gross_usd\nA,2001,10\nA,2001,10\n",
        encoding="utf-8",
    )
    item = {
        "provider": "boxofficemetrics",
        "batch_id": "b1",
        "feed": "domestic_csv",
        "file": str(p),
        "source_mod_time": "2025-01-01T00:00:00Z",
        "file_hash": "h123",
    }

    class L:
        def info(self, *a, **k):
            pass

        def warning(self, *a, **k):
            pass

        def error(self, *a, **k):
            pass

        def debug(self, *a, **k):
            pass

    logger = L()

    ingest_csv(item, mappings, contracts, logger)
    out_dir = temp_project["bronze"] / "boxofficemetrics" / "domestic_csv" / "b1"
    files = list(out_dir.glob("*.csv"))
    assert files, "no bronze CSVs written"
    consolidate_bronze_feed("boxofficemetrics", "domestic_csv", "b1", logger)
    assert (out_dir / "consolidated.csv").exists()


def test_ingest_json_generic(temp_project):
    contracts = {
        "entities": {
            "audiencepulse_ratings_stage": {
                "columns": [
                    {"name": "provider"},
                    {"name": "feed"},
                    {"name": "batch_id"},
                    {"name": "source_file"},
                    {"name": "source_mod_time"},
                    {"name": "file_hash"},
                    {"name": "ingest_ts"},
                    {"name": "schema_version"},
                    {"name": "record_hash"},
                    {"name": "movie_key"},
                    {"name": "title"},
                    {"name": "year"},
                    {"name": "audience_average_score"},
                    {"name": "total_audience_ratings"},
                ]
            }
        }
    }
    mappings = {
        "providers": {
            "audiencepulse": {
                "schema_version": 1,
                "feeds": {
                    "ratings_json": {
                        "input_format": "json",
                        "mappings": {
                            "title": {"to": "title", "type": "string"},
                            "year": {"to": "year", "type": "int"},
                            "audience_average_score": {
                                "to": "audience_average_score",
                                "type": "float",
                            },
                            "total_audience_ratings": {
                                "to": "total_audience_ratings",
                                "type": "int",
                            },
                        },
                        "record_identity": {
                            "columns": [
                                "title",
                                "year",
                                "audience_average_score",
                                "total_audience_ratings",
                            ]
                        },
                        "target_entity": "audiencepulse_ratings_stage",
                    }
                },
            }
        }
    }
    incoming = temp_project["incoming"] / "audiencepulse" / "b1"
    incoming.mkdir(parents=True, exist_ok=True)
    j = incoming / "provider2.jsonl"
    j.write_text(
        '{"title": "A", "year":2001,"audience_average_score":9.1,"total_audience_ratings":10}\n',
        encoding="utf-8",
    )
    item = {
        "provider": "audiencepulse",
        "batch_id": "b1",
        "feed": "ratings_json",
        "file": str(j),
        "source_mod_time": "2025-01-01T00:00:00Z",
        "file_hash": "hh",
    }

    class L:
        def info(self, *a, **k):
            pass

        def warning(self, *a, **k):
            pass

        def error(self, *a, **k):
            pass

        def debug(self, *a, **k):
            pass

    logger = L()

    ingest_json(item, mappings, contracts, logger)
    out_dir = temp_project["bronze"] / "audiencepulse" / "ratings_json" / "b1"
    files = list(out_dir.glob("*.csv"))
    assert files, "no bronze JSON CSVs written"
