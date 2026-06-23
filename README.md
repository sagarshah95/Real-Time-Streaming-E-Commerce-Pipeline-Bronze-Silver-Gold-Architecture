# E-Commerce Real-Time Data Pipeline

A modern, end-to-end data engineering pipeline demonstrating real-time ingestion, stream processing, and storage using **Apache Kafka**, **Apache Spark Structured Streaming**, and **Delta Lake** structured around the **Medallion Architecture**.

> Looking for a deep, file-by-file and terminal-by-terminal explanation? See **`PROJECT_DOCUMENTATION.md`**. This README is the quick setup-and-run guide.

---

## ── Architecture Overview

This project simulates a real-time e-commerce clickstream data pipeline, scaling from raw ingestion to business-ready KPIs.
```
[Kafka Producer]
     │  (e-commerce events as JSON)
     ▼
[Apache Kafka]  ◄── KRaft mode, single broker
     │
     ▼
[Spark Structured Streaming]
     │
     ├──► Bronze Layer (Delta Lake) — raw events, append-only
     │
     ├──► Silver Layer (Delta Lake) — cleaned, deduplicated, typed
     │
     └──► Gold Layer  (Delta Lake) — windowed aggregations, KPIs

All Delta tables physically live in MinIO (S3-compatible) under  s3a://lakehouse/
```

### 🛠️ Tech Stack
* **Orchestration:** Docker & Docker Compose
* **Ingestion:** Apache Kafka (KRaft mode, ZooKeeperless)
* **Stream Processing:** Apache Spark Structured Streaming (Spark 3.5.1)
* **Storage Layer:** Delta Lake 3.2.0 on MinIO (S3-compatible Object Storage)
* **Data Generation:** Python (Faker + confluent-kafka)

---

## ── Pipeline Breakdown

### 1. Ingestion Layer (Kafka)
* A Python script uses the `Faker` library to generate mock e-commerce clickstream events (login, click_nav, purchase, logout).
* Events are continuously published to a Kafka topic named `ecommerce-events`.
* The Kafka broker runs inside a Docker container using **KRaft mode** (no ZooKeeper).

### 2. Medallion Architecture (Spark & Delta Lake)

#### 🟫 Bronze Layer (Raw Ingestion) — `spark_jobs/bronze_ingest.py`
* **Objective:** Capture the raw stream immediately with minimal overhead.
* **Implementation:** Ingests the raw Kafka payload and appends it directly to a Delta table backed by MinIO.
* **Fault Tolerance:** Spark **checkpointing** tracks Kafka offsets for exactly-once guarantees and restart-resume.
* **Trigger:** `processingTime` micro-batch trigger of **10 seconds**.

#### ⬜ Silver Layer (Cleaned & Structured) — `spark_jobs/silver_clean.py`
* **Objective:** Structure, clean, and enrich the raw data.
* **Implementation:** Parses the raw JSON into a typed schema, casts `event_timestamp` to a real timestamp, drops null/duplicate records, and adds a `processed_at` timestamp for observability.
* **Deduplication:** Uses a **5-minute watermark** on event time to drop duplicates from the state store.
* **Startup safety:** Bootstraps the Silver Delta table schema on launch so the Gold stream can attach immediately (avoids `DELTA_SCHEMA_NOT_SET`).

#### 🟨 Gold Layer (Aggregations & KPIs) — `spark_jobs/gold_aggregate.py`
* **Objective:** Produce high-level, business-ready metrics.
* **Implementation:** Computes KPIs (total events, unique users, total revenue, purchase count, average order value, conversion rate) grouped by **5-minute tumbling windows**.
* **Late Data Handling:** A **10-minute watermark** allows late-arriving events into window calculations before the state is finalized and evicted.

---

## ── Prerequisites

* **Docker Desktop** (with the WSL2 backend on Windows). Allocate at least **6–8 GB RAM** and **4 CPUs** in Docker Desktop → Settings → Resources, since three streaming jobs run concurrently.
* **Python 3.10+** on your host (only needed to run the producer). Python 3.13 is supported.
* The commands below are written for **Windows PowerShell** (single-line). On macOS/Linux they work the same; you may use `\` line-continuation if you prefer.

---

## ── Setup & Run

### Step 0. The `.env` file
A working `.env` already exists in the project root (it is git-ignored). It is consumed by Docker Compose, the Spark jobs, and the producer. If you ever need to recreate it:

```bash
# MinIO root credentials (used by the minio service in docker-compose)
# Keep as minioadmin/minioadmin so the minio-init container can create the bucket.
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Used by the Spark jobs (run INSIDE the docker network)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
KAFKA_BOOTSTRAP_SERVER=kafka:9092
TOPIC=ecommerce-events

# Used by the producer (runs on your HOST machine)
BOOTSTRAP_SERVER=localhost:29092
```

### Step 1. Start the infrastructure
From the project root, build the custom Spark image and bring up Kafka, MinIO, the bucket initializer, and Spark master + worker:

```powershell
docker compose up -d --build
```

The first build downloads the Spark base image and several JARs, so it can take a few minutes. Then confirm everything is up:

```powershell
docker compose ps
```

You want `kafka` and `minio-lakehouse` showing `(healthy)`, `spark-master` and `spark-worker` `Up`, and `minio-init` `Exited (0)` (it runs once to create the `lakehouse` bucket, then stops — that's expected).

Useful UIs:
* Spark master UI → http://localhost:8080 (should show 1 worker registered)
* MinIO console → http://localhost:9001 (login `minioadmin` / `minioadmin`)

### Step 2. Start the producer (in its own terminal)
Run from the project root so `python-dotenv` finds `.env`:

```powershell
pip install --only-binary :all: -r producer/requirements.txt
python producer/producer.py
```

The `--only-binary :all:` flag forces pip to use a prebuilt wheel (important on Windows/Python 3.13). You should see a stream of `Delivered:{...}` lines. **Leave this running** — if it stops, the whole pipeline goes idle.

### Step 3. Submit the Spark streaming jobs (each in its own terminal, in order)
These are long-running streams that feed each other, so start them in order and give each ~30–60s to write data before starting the next.

**Bronze** (Kafka → Delta):

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/bronze_ingest.py
```

**Silver** (clean + dedup; start after Bronze has written some data):

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/silver_clean.py
```

**Gold** (windowed KPIs; start after Silver has written some data):

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_aggregate.py
```

Each job is pinned to **1 core / 512 MB** so all three fit inside the 2 GB worker. Check `http://localhost:8080` — every running app should show a granted executor (1 core, 512 MB), not be stuck `WAITING`.

### Step 4. Verify the layers
A one-shot batch job that prints row counts for all three layers (run it any time):

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_test.py
```

Example output:

```
bronze_layer: 2506 rows
silver_layer: 342 rows
gold_layer: 342 rows
```

You can also browse the actual Delta files in the MinIO console under `lakehouse/bronze`, `lakehouse/silver`, and `lakehouse/gold`.

### Step 5. Shut down
```powershell
docker compose down       # stop containers, keep data volumes
docker compose down -v    # also remove kafka + minio data volumes (clean slate)
```

---

## ── Troubleshooting

**`pip install` fails building `confluent-kafka` ("filename or extension is too long").**
You're on a Python version without a matching prebuilt wheel and pip tried to compile from source. Use the wheel-only install: `pip install --only-binary :all: -r producer/requirements.txt`. `confluent-kafka` is pinned `>=2.6.0` for Python 3.13 wheel support.

**`PATH_NOT_FOUND: s3a://lakehouse/gold/...` when running `gold_test.py`.**
The Gold table hasn't been written yet. Make sure Bronze → Silver → Gold are all running and Gold has committed at least one batch. The verification script now reports a missing layer gracefully instead of crashing.

**`DELTA_SCHEMA_NOT_SET` when starting Gold.**
Gold started before Silver had a schema. Silver now bootstraps its table schema on startup — restart the Silver job (so the new code runs), then start Gold.

**`Initial job has not accepted any resources; ... workers are registered and have sufficient resources`.**
The worker (4 cores / 2 GB) ran out of memory. The jobs are capped at `512m` each so three fit in 2 GB. If you add jobs or want headroom, raise Docker Desktop RAM and `SPARK_WORKER_MEMORY` in `docker-compose.yaml`, then `docker compose up -d` to recreate the worker.

**`WARN ProcessingTimeExecutor: Current batch is falling behind`.**
Backpressure, not an error — writing Delta to MinIO + checkpointing is slow on a small local box. Spark simply runs the next batch immediately.

**Gold has roughly one row per event / aggregates look meaningless.**
The producer uses `fake.iso8601()`, which scatters `event_timestamp` across decades, so each event lands in its own 5-minute window. To get meaningful aggregates, emit current time instead:

```python
from datetime import datetime, timezone
# ...
"event_timestamp": datetime.now(timezone.utc).isoformat()
```

Then restart the producer.

---

## ── Key Takeaways & Observations
* **Resource Constraints:** Running concurrent, stateful streaming jobs (deduplication + windowed aggregations) inside a local Docker environment quickly highlights local CPU and memory bottlenecks — hence the 512 MB-per-executor sizing.
* **State Management:** Fine-tuning watermarks (5-minute vs. 10-minute thresholds) is critical to maintaining a healthy memory footprint and preventing unbounded state store growth.
* **Scaling:** This setup is ideal for learning; production-grade scaling means moving Spark to a distributed environment (e.g. Databricks) with managed Kafka and cloud object storage.
