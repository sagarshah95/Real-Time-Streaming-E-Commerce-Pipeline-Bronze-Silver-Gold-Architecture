# E-Commerce Real-Time Data Pipeline

A modern, end-to-end data engineering pipeline demonstrating real-time ingestion, stream processing, and storage using **Apache Kafka**, **Apache Spark Structured Streaming**, and **Delta Lake** structured around the **Medallion Architecture**.

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
```

### 🛠️ Tech Stack
* **Orchestration:** Docker & Docker Compose
* **Ingestion:** Apache Kafka (KRaft mode, ZooKeeperless)
* **Stream Processing:** Apache Spark Structured Streaming
* **Storage Layer:** Delta Lake on MinIO (S3-compatible Object Storage)
* **Data Generation:** Python (Faker library)

---

## ── Pipeline Breakdown

### 1. Ingestion Layer (Kafka)
* A Python script utilizes the `Faker` library to generate mock e-commerce clickstream events (e.g., user logins, product views, cart additions, purchases).
* Events are continuously published to a Kafka topic named `ecommerce-events`.
* The Kafka broker runs inside a Docker container utilizing **KRaft mode** for cluster management.

### 2. Medallion Architecture (Spark & Delta Lake)

#### 🟫 Bronze Layer (Raw Ingestion)
* **Objective:** Capture the raw stream immediately with minimal overhead.
* **Implementation:** Ingests the raw JSON payload from Kafka and appends it directly to a Delta Lake table backed by MinIO storage.
* **Fault Tolerance:** Implements Spark **checkpointing** to track offsets, ensuring exactly-once processing guarantees.
* **Trigger:** Configured with a `processingTime` micro-batch trigger of **10 seconds**.

#### ⬜ Silver Layer (Cleaned & Structured)
* **Objective:** Structure, clean, and enrich the raw data for downstream consumption.
* **Implementation:** Parses the raw JSON string into a strongly-typed schema and flattens nested elements. Adds an execution timestamp (`processed_at`) for observability.
* **Deduplication:** Utilizes a **5-minute watermark** on the event time to dynamically drop duplicate `eventId` records from the state store.

#### 🟨 Gold Layer (Aggregations & KPIs)
* **Objective:** Produce high-level, business-ready metrics.
* **Implementation:** Computes real-time KPIs (e.g., total purchases, active users) grouped by **5-minute tumbling windows**.
* **Late Data Handling:** Employs a **10-minute watermark** to allow late-arriving events to be factored into window calculations before the state is finalized and evicted.

---

## ── Local Development Setup

### Prerequisites
* Docker & Docker Compose
* Python 3.10+ (for local producer simulation)

### 0. Create the `.env` file
Create a `.env` file next to `docker-compose.yaml` (it is git-ignored and required by both Compose and the apps):

```bash
# MinIO root credentials (used by the minio service in docker-compose)
# Keep as minioadmin/minioadmin so the minio-init container can create the bucket.
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Used by the Spark jobs (run inside the docker network)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
KAFKA_BOOTSTRAP_SERVER=kafka:9092
TOPIC=ecommerce-events

# Used by the producer (runs on your host machine)
BOOTSTRAP_SERVER=localhost:29092
```

### 1. Spin up the Infrastructure
Build the custom Spark image and bring up Kafka, MinIO, Spark Master, and Spark Worker containers:

```bash
docker compose up -d --build
```

Note: A helper container (`minio-init`) automatically runs to create the `lakehouse` bucket inside MinIO on startup, then exits.

Useful UIs once healthy:
* Spark master UI → http://localhost:8080
* MinIO console → http://localhost:9001 (login `minioadmin` / `minioadmin`)

### 2. Start the Stream Producer
Install requirements and start generating fake clickstream traffic into Kafka (run from the project root so `python-dotenv` finds `.env`):

```bash
pip install -r producer/requirements.txt
python producer/producer.py
```

### 3. Submit Spark Streaming Jobs
Each job is a long-running stream — run each in its own terminal, in order (Bronze → Silver → Gold), since Silver reads Bronze and Gold reads Silver:

```bash
# Bronze (Kafka -> Delta)
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark_jobs/bronze_ingest.py

# Silver (clean + dedup) -- start after Bronze has written some data
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark_jobs/silver_clean.py

# Gold (windowed KPIs) -- start after Silver has written some data
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark_jobs/gold_aggregate.py
```

### 4. Verify the Layers
Run the one-shot verification job to print row counts for all three layers:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark_jobs/gold_test.py
```

### 5. Shut Down
```bash
docker compose down       # stop containers, keep data volumes
docker compose down -v    # also remove kafka + minio data volumes (clean slate)
```

---

## ── Key Takeaways & Observations
* **Resource Constraints:** Running concurrent, stateful streaming jobs (deduplication + windowed aggregations) inside a local Docker environment quickly highlights local CPU and memory bottlenecks.
* **State Management:** Fine-tuning watermarks (5-minute vs. 10-minute thresholds) is critical to maintaining a healthy memory footprint and preventing unbounded state store growth.