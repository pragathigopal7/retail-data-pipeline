LAKE ?= lake
ORDERS ?= 5000

.PHONY: install test lint demo kafka-up kafka-demo kafka-down clean

install:
	pip install -e ".[dev,kafka]"

test:
	pytest -q

lint:
	ruff check src tests && ruff format --check src tests

demo:            ## file mode, no infrastructure needed
	retail-pipeline --lake $(LAKE) run --orders $(ORDERS)

kafka-up:
	docker compose up -d

kafka-demo: kafka-up
	retail-pipeline produce --orders $(ORDERS)
	retail-pipeline --lake $(LAKE) ingest --kafka localhost:19092
	retail-pipeline --lake $(LAKE) transform
	retail-pipeline --lake $(LAKE) check

kafka-down:
	docker compose down -v

clean:
	rm -rf $(LAKE) data .pytest_cache
