PYTHON ?= python

install:
	$(PYTHON) -m pip install -e .[dev]

fmt:
	ruff format .

lint:
	ruff check .

fix:
	ruff check . --fix
	ruff format .

test:
	pytest -q

seed:
	mytel seed sample-data --output sample_data/generated_events.jsonl --records 200

produce:
	mytel produce --input sample_data/generated_events.jsonl

consume:
	mytel consume --max-batches 5 --batch-size 50

api:
	mytel api --host 0.0.0.0 --port 8080

run-local:
	$(MAKE) seed
	$(MAKE) produce
	$(MAKE) consume
