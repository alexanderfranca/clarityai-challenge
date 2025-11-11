VENV ?= .venv
PYTHON ?= python3.10
PYTHONPATH ?= src

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make setup     - create venv and install deps"
	@echo "  make run_etl   - run the pipeline end-to-end"
	@echo "  make test      - run all tests"
	@echo "  make clean     - remove build artifacts and tmp files"

$(VENV)/bin/activate: requirements.txt requirements-dev.txt
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -U pip
	$(VENV)/bin/pip install -r requirements.txt
	$(VENV)/bin/pip install -r requirements-dev.txt
	touch $(VENV)/bin/activate

.PHONY: setup
setup: $(VENV)/bin/activate

.PHONY: run_etl
run_etl: setup
	PYTHONPATH=$(PYTHONPATH) $(VENV)/bin/python -m src.main run_etl

.PHONY: test
test: setup
	PYTHONPATH=$(PYTHONPATH) $(VENV)/bin/python -m pytest -q

.PHONY: clean
clean:
	rm -rf $(VENV) .pytest_cache .ruff_cache **/__pycache__
