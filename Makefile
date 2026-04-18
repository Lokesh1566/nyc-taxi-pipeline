# Common commands for the pipeline.
# Run `make help` to see all targets.

PYTHON := python
SPARK_SUBMIT := spark-submit --master local[4] --driver-memory 4g

.PHONY: help install download stream ingest transform gold validate snowflake dashboard test clean up down logs

help:
	@echo "NYC Taxi Pipeline — make targets"
	@echo ""
	@echo "  install        pip install -r requirements.txt"
	@echo "  download       download TLC data (year=2023 month=6 by default)"
	@echo "  stream         start the streaming data simulator"
	@echo "  ingest         run spark structured streaming (bronze)"
	@echo "  transform      run bronze -> silver transformation"
	@echo "  gold           run silver -> gold aggregations"
	@echo "  validate       run GE validation on silver"
	@echo "  snowflake      load gold tables into Snowflake"
	@echo "  dashboard      launch the Streamlit dashboard"
	@echo "  test           run pytest"
	@echo "  up             docker-compose up -d"
	@echo "  down           docker-compose down"
	@echo "  logs           tail docker-compose logs"
	@echo "  clean          remove data/processed and data/checkpoints"
	@echo ""
	@echo "Quick demo (no docker):"
	@echo "  make install && make download && make transform && make gold && make dashboard"

install:
	$(PYTHON) -m pip install -r requirements.txt

# override with `make download YEAR=2024 MONTH=3`
YEAR ?= 2023
MONTH ?= 6
download:
	$(PYTHON) scripts/download_tlc_data.py --year $(YEAR) --month $(MONTH)

# assumes the downloaded file exists; adjust filename if needed
stream:
	$(PYTHON) scripts/generate_stream.py \
		--source data/raw/yellow_tripdata_$(YEAR)-$(shell printf '%02d' $(MONTH)).parquet \
		--output-dir data/raw/streaming \
		--batch-size 5000 \
		--interval 2

ingest:
	$(SPARK_SUBMIT) spark_jobs/stream_ingestion.py

# for quick dev we often skip streaming and just read the raw parquet directly.
# this target treats the downloaded file as if it were bronze.
transform:
	@mkdir -p data/processed/bronze
	@cp -n data/raw/yellow_tripdata_*.parquet data/processed/bronze/ 2>/dev/null || true
	$(SPARK_SUBMIT) spark_jobs/bronze_to_silver.py

gold:
	$(SPARK_SUBMIT) spark_jobs/silver_to_gold.py

validate:
	$(PYTHON) -m tests.great_expectations.run_validation --dataset silver

snowflake:
	$(PYTHON) scripts/snowflake_loader.py --setup --load

dashboard:
	streamlit run dashboards/streamlit_app.py

test:
	pytest tests/ -v --tb=short

up:
	docker-compose up -d
	@echo "Airflow UI:   http://localhost:8080  (admin/admin)"
	@echo "Spark UI:     http://localhost:8081"

down:
	docker-compose down

logs:
	docker-compose logs -f --tail=100

clean:
	rm -rf data/processed data/checkpoints
	@echo "cleaned processed data and checkpoints (raw data kept)"
