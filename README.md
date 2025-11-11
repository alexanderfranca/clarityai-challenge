ClarityAI Data Engineering Challenge
====================================


TL;DR — Quick Start
===================
```
git clone https://github.com/alexanderfranca/clarityai-challenge.git
cd clarityai-challenge
make setup       # create venv and install dependencies
make test        # run unit tests
make run_etl     # run the full ETL pipeline (bronze → silver → gold)
```

Query the generated gold data
-----------------------------
```
python src/main.py query --list 10
python src/main.py query --title "Inception" --year 2010
python src/main.py query --key MOV12345
```
---

Project Overview
================
This repository implements an end-to-end ETL pipeline that ingests multiple movie data sources and produces a curated “gold” dataset ready for data analysts.

The architecture follows the Bronze–Silver–Gold layered pattern.

Features
========
* Three-tier ETL (Bronze / Silver / Gold)
* Pure text format (CSV and JSON only — easy to inspect, no infra dependencies)
* Automatic detection of already-processed batches
* Schema drift detection using YAML contracts
* Duplicate handling
* Idempotent execution — safe to re-run without side effects
* Configuration-driven — new providers defined via YAML (contracts.yaml, mappings.yaml, paths.yaml)
* Audit log of ingested batches
* Lightweight querying interface for analysts (movie-pipeline query)

---

Missing / Future Improvements
=============================
* Test coverage could be higher.
* Tests were written after implementation (not strict TDD).
* Performance for large-scale data not yet optimized.
* No cleanup policy yet — generated files accumulate.
* Error recovery can be improved.

---

Adding New Sources
==================
Place new data under:

data/incoming/<provider_name>/<batch_id>/


Each batch must include an empty _READY file to signal “landing complete.”

Example:
```
data/incoming/criticagg/2025-01-01/
├── reviews.csv
└── _READY
```
---

Adding a New Provider
=====================
1. Define schema in configs/contracts.yaml.
2. Map fields in configs/mappings.yaml.
3. Add paths to configs/paths.yaml.

---

Repository Structure
====================
```
clarityai-challenge/
├── configs/
├── data/
├── src/
│   ├── core/
│   │   ├── ingest.py
│   │   ├── silver.py
│   │   ├── gold.py
│   │   ├── query.py
│   ├── utils/
│   ├── main.py
├── tests/
├── Makefile
├── pyproject.toml
└── README.md
```
---

Message to the Review Board
===========================
Dear ClarityAI team,

Thank you for the opportunity to complete this assessment.

Key design choices:
* Focused on clarity, correctness, and extensibility.
* Pure Python + YAML configs — no infra dependencies.
* Bronze–Silver–Gold architecture.
* Traceable, idempotent execution with audits.
* Config-as-code for new providers.


Areas for improvement:
* Increase test coverage.
* Adopt stricter TDD.
* Consider Parquet + partitioned ingestion for scalability.
* Future integration with Airflow/Composer and schema evolution.


Best regards,  
Alexander da Franca Fernandes

---

License
=======
MIT (for assessment purposes)

---

Included Sample Data
====================
This repository ships with small sample files under data/incoming/.


They allow the pipeline to run end-to-end immediately after cloning:

```
make setup
make run_etl
python src/main.py query --list 10
```


The pipeline automatically detects the _READY files and processes them into bronze, silver, and gold layers.

