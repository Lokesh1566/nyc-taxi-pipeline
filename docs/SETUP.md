# Setup

Step-by-step setup including the annoying bits I hit along the way.

## Prerequisites

- Python 3.10 or 3.11 (3.12 has some pyspark issues, avoid for now)
- Java 11 (not 17 — PySpark 3.5 has issues with Java 17 on Apple Silicon)
- Docker Desktop (if you want to run Airflow)
- ~5GB of free disk (data + checkpoints + docker images)
- A Snowflake account (free trial works)

## 1. Clone and install

```bash
git clone https://github.com/Lokesh1566/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

python -m venv venv
source venv/bin/activate    # or: venv\Scripts\activate on windows

pip install -r requirements.txt
```

## 2. Java setup (the part that wasted 3 hours of my life)

On Apple Silicon I had to use Java 11 specifically. The symptom if you're
on Java 17 is cryptic errors from Arrow — something about `sun.misc.Unsafe`.

```bash
# mac: install Java 11 via homebrew
brew install openjdk@11

# add to your shell profile (.zshrc or .bash_profile)
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
export PATH="$JAVA_HOME/bin:$PATH"

# verify
java -version
# should say: openjdk version "11.0.x"
```

On Linux:
```bash
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

On Windows you're on your own, sorry. I don't have a Windows machine to test on.

## 3. Get some data

The free NYC TLC data lives on Cloudflare. One month of yellow taxi data
is about 150MB.

```bash
python scripts/download_tlc_data.py --year 2023 --month 06
```

This also grabs the zone lookup CSV (small, ~5KB).

## 4. Set up Snowflake

Sign up for a free trial at https://signup.snowflake.com (no credit card needed).

Once you're in:
1. Your account identifier is in the URL: e.g. `abc12345.us-east-1.snowflakecomputing.com`
   → account = `abc12345.us-east-1`
2. Create a database:
   ```sql
   CREATE DATABASE TAXI_PIPELINE;
   ```
3. Copy the config:
   ```bash
   cp config/snowflake_config.example.yaml config/snowflake_config.yaml
   ```
4. Fill in `config/snowflake_config.yaml` with your username, password, and account.

Test the connection:
```bash
python scripts/snowflake_loader.py --setup
```

If this runs without errors, you're good. If you get `HTTP 403` errors, it's
almost always the account identifier format — try without the region suffix.

## 5. Run the pipeline (without docker)

The quickest way to see everything working:

```bash
# copy the raw file into bronze (skipping streaming for the first run)
mkdir -p data/processed/bronze
cp data/raw/yellow_tripdata_2023-06.parquet data/processed/bronze/

# run the transforms
make transform
make validate
make gold

# load to snowflake
make snowflake

# view the dashboard
make dashboard
```

Open http://localhost:8501 for the dashboard.

## 6. Run the pipeline with streaming (optional)

If you want to see the streaming in action:

```bash
# terminal 1: generate stream
python scripts/generate_stream.py \
    --source data/raw/yellow_tripdata_2023-06.parquet \
    --batch-size 5000 \
    --interval 2

# terminal 2: run spark streaming
python spark_jobs/stream_ingestion.py
```

You'll see parquet files appear in `data/processed/bronze/` as the stream
processes them. Ctrl+C to stop.

## 7. Run with Airflow (optional)

If you want to run the orchestrated version:

```bash
# first time only
echo "AIRFLOW_UID=$(id -u)" > .env

# start everything
make up

# give it a minute, then open http://localhost:8080 (admin/admin)
```

You should see three DAGs:
- `taxi_ingestion_monitor` (runs every 15 min, paused by default)
- `taxi_transform_pipeline` (daily at 2am)
- `taxi_data_quality` (daily at 4am)

Unpause and trigger manually to test.

Stop everything:
```bash
make down
```

## Troubleshooting

### "JAVA_HOME not set" when running pyspark

See section 2 above. Make sure you've exported `JAVA_HOME` in your current
shell (sourcing .zshrc helps).

### Spark hangs forever on `readStream`

Usually means the input directory is empty. Spark's file streaming source
just sits there waiting for files to appear. Run the stream generator in
another terminal.

### Snowflake connection hangs

If you're on a VPN or behind a corporate proxy, the default connection
timeout is way too long. The loader already sets `login_timeout=30` which
helps. If it still hangs, try:

```python
# in scripts/snowflake_loader.py, add to the connect() call:
proxy_host='your-proxy.example.com',
proxy_port=8080,
```

### "No module named pyspark" in Airflow

The docker-compose installs extra requirements on container start via
`_PIP_ADDITIONAL_REQUIREMENTS`. If you changed requirements, you need to
recreate the containers:

```bash
docker-compose down
docker-compose up -d --force-recreate
```

### DAG not showing up in Airflow UI

Airflow's DAG refresh is cached. Restart the scheduler:

```bash
docker-compose restart airflow-scheduler
```

Give it about 30 seconds and refresh the UI.

### Streamlit dashboard shows "could not load KPIs"

Means the gold tables are empty or don't exist in Snowflake. Make sure you've run:

```bash
make snowflake
```

And check in Snowflake's web UI that the tables have rows:
```sql
SELECT COUNT(*) FROM FCT_TRIPS_DAILY;
```

### Apple Silicon — `illegal instruction` crash

Happens occasionally with older pyarrow. Upgrade:

```bash
pip install --upgrade pyarrow
```

## Where to go next

- Check out `docs/ARCHITECTURE.md` for the design rationale
- `docs/BENCHMARKS.md` has performance numbers and tuning notes
- Open an issue if something's not working — I'd rather hear about it
