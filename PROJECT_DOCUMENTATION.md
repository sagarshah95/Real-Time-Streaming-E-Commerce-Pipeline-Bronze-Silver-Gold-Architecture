# E-Commerce Real-Time Data Pipeline — Detailed Documentation

This document explains **every file** in the project and **every terminal** you run, in
depth, with concrete examples taken from a real run of the system. It is meant to be a
complete mental model: after reading it you should understand what each piece does, why it
exists, how data flows end-to-end, and how to read the logs you see in each terminal.

> If you just want to run the project, see `README.md`. This file is the "how and why".

---

## 1. The 10,000-foot view

The project simulates a live e-commerce **clickstream** (logins, navigation clicks,
purchases, logouts) and pushes it through a streaming lakehouse built on the
**Medallion Architecture** (Bronze → Silver → Gold).

```
[producer.py]            generates fake events, 1 per second
     │  JSON over the network
     ▼
[Kafka]  topic: ecommerce-events     (message broker / buffer)
     │
     ▼
[Spark: bronze_ingest.py]  reads Kafka  ─► writes RAW bytes  ─► Bronze Delta table
     │
     ▼
[Spark: silver_clean.py]   reads Bronze ─► parse + clean + dedup ─► Silver Delta table
     │
     ▼
[Spark: gold_aggregate.py] reads Silver ─► windowed KPIs ─► Gold Delta table
     │
     ▼
[gold_test.py]  one-shot batch job that counts rows in all three tables

All three Delta tables physically live in MinIO (an S3-compatible object store)
under the bucket  s3a://lakehouse/ .
```

Everything except the producer runs **inside Docker containers**. The producer runs on
your Windows host and talks to Kafka through the published port `localhost:29092`.

---

## 2. End-to-end example: following one event through the pipeline

This is the single most useful thing to understand. Let's trace one real record that the
producer emitted during your run:

```json
{
  "user_id": "8ab8597c-da8d-4768-b4f6-aaa9fe059b25",
  "event_type": "login",
  "product_id": 8457,
  "amount": 4739,
  "event_timestamp": "1990-10-23T02:10:30"
}
```

**Stage 1 — Producer → Kafka.** `producer.py` serializes this dict to a JSON byte string
and calls `producer.produce(topic="ecommerce-events", value=<bytes>)`. Kafka stores it as
one message in the `ecommerce-events` topic. You see it confirmed in the producer terminal:
`Delivered:{"user_id": "8ab8597c-...", ...}`.

**Stage 2 — Kafka → Bronze.** `bronze_ingest.py` reads the message as a generic Kafka row.
Bronze does **not** look inside the JSON; it stores the envelope verbatim. The Bronze Delta
row looks like:

| key  | value (binary)                  | topic             | partition | offset | timestamp (kafka) |
|------|---------------------------------|-------------------|-----------|--------|-------------------|
| null | `{"user_id":"8ab8...","amount":4739,...}` | ecommerce-events | 0 | 1041 | 2026-06-23 02:10:21 |

This is the "landing zone": cheap, append-only, loss-proof. If a downstream layer has a
bug, the raw truth is always preserved here.

**Stage 3 — Bronze → Silver.** `silver_clean.py` parses the `value` bytes back into JSON
using a declared schema, flattens it into real typed columns, casts `event_timestamp` from
string to a real `timestamp`, drops nulls/duplicates, and stamps `processed_at`:

| user_id | event_type | product_id | amount | event_timestamp     | processed_at        |
|---------|------------|------------|--------|---------------------|---------------------|
| 8ab8... | login      | 8457       | 4739.0 | 1990-10-23 02:10:30 | 2026-06-23 02:16:00 |

**Stage 4 — Silver → Gold.** `gold_aggregate.py` groups Silver rows into **5-minute
windows** based on `event_timestamp` and computes KPIs. Our example falls into the window
`[1990-10-23 02:10:00, 1990-10-23 02:15:00)`:

| window_start        | window_end          | total_events | unique_users | total_revenue | purchase_count | avg_order_value | conversion_rate |
|---------------------|---------------------|--------------|--------------|---------------|----------------|-----------------|-----------------|
| 1990-10-23 02:10:00 | 1990-10-23 02:15:00 | 1            | 1            | 0.0           | 0              | null            | 0.0             |

(`total_revenue`/`purchase_count` are 0 here because this event is a `login`, not a
`purchase`.)

> **Important real-world caveat seen in your run:** the producer uses
> `fake.iso8601()`, which generates timestamps scattered randomly across ~1970–2026. Because
> Gold buckets by *event time*, almost every event lands in its own unique 5-minute window —
> so Gold ends up with roughly **one row per event** instead of meaningful aggregates. That
> is exactly why your `gold_layer` row count (342) matched your `silver_layer` count (342).
> See §6 for the one-line fix that makes windows aggregate properly.

---

## 3. File-by-file reference

### 3.1 `docker-compose.yaml` — the orchestrator

Defines the whole local cluster as 5 services on a shared bridge network `lakehouse-net`.
Services on the same network reach each other by service name (e.g. `kafka`, `minio`).

| Service       | Image                          | Role |
|---------------|--------------------------------|------|
| `kafka`       | `confluentinc/cp-kafka:7.8.3`  | Single-broker Kafka in **KRaft mode** (no ZooKeeper). Acts as both broker and controller. |
| `minio`       | `minio/minio:latest`           | S3-compatible object store. This is where all Delta data physically lives. |
| `minio-init`  | `minio/mc:latest`              | Run-once helper: creates the `lakehouse` bucket, then exits. |
| `spark-master`| built from `Dockerfile`        | Spark Standalone master + the box you `spark-submit` jobs from. |
| `spark-worker`| built from `Dockerfile`        | Spark Standalone worker that actually runs executors. |

Key details that matter for *why things behave the way they do*:

- **Kafka dual listeners.** `KAFKA_ADVERTISED_LISTENERS` exposes two addresses:
  - `kafka:9092` — used *inside* Docker (Spark connects here).
  - `localhost:29092` — published to your host so the producer on Windows can connect.
  This split is the whole reason the producer uses `BOOTSTRAP_SERVER=localhost:29092` while
  Spark uses `KAFKA_BOOTSTRAP_SERVER=kafka:9092`.
- **Healthchecks.** `spark-master`/`spark-worker` wait for Kafka and MinIO to be `healthy`
  before starting. `minio-init` waits for MinIO to be healthy before creating the bucket.
- **Worker sizing.** `SPARK_WORKER_MEMORY=2g`, `SPARK_WORKER_CORES=4`. This is the *total*
  resource pool every job shares. (See §5 for how 3 jobs fit into it.)
- **Volumes.** `kafka_kraft` and `minio_data` persist data across `docker compose down`.
  Use `docker compose down -v` to wipe them for a clean slate.
- **Bind mount.** `./spark_jobs:/opt/spark_jobs` maps your local job files into both Spark
  containers, so editing a `.py` file locally + re-submitting picks up changes immediately
  (no rebuild needed).

### 3.2 `Dockerfile` — the custom Spark image

Starts from `apache/spark:3.5.1` and adds the JARs Spark needs that are **not** bundled by
default:

- `delta-spark` + `delta-storage` (3.2.0) → lets Spark read/write **Delta Lake** tables.
- `spark-sql-kafka` + `kafka-clients` + `commons-pool2` → lets Spark read from **Kafka**.
- `hadoop-aws` + `aws-java-sdk-bundle` → lets Spark talk to **S3/MinIO** via the `s3a://`
  filesystem.

It also `pip install python-dotenv`. Without these JARs, the jobs would fail with
`ClassNotFoundException` / "Failed to find data source: delta|kafka".

### 3.3 `.env` — configuration & secrets

A single file consumed in two different ways:

- By **docker-compose** (for the `minio` service root credentials).
- By the **Spark jobs** via `env_file: .env` in compose (so `os.getenv(...)` works inside
  the containers) and by the **producer** via `python-dotenv` on the host.

The split between `MINIO_ENDPOINT=http://minio:9000` (in-network) and
`BOOTSTRAP_SERVER=localhost:29092` (host) mirrors the dual-listener design above.

> This file is in `.gitignore` — it should never be committed. In production these become
> real secrets (S3 keys, Kafka SASL credentials) stored in a secret manager.

### 3.4 `.gitignore`

Excludes `.env`, `PROJECT_PLAN.md`, `venv`, `__pycache__`, and a stray
`docker-compose.yml`. Standard hygiene so secrets and local junk don't get committed.

### 3.5 `producer/producer.py` — the data generator

A plain Python script (runs on the host, not in Docker). Responsibilities:

1. Read `BOOTSTRAP_SERVER` and `TOPIC` from `.env`.
2. Create a `confluent_kafka.Producer` pointed at `localhost:29092`.
3. In an infinite loop, build a fake event with `Faker` and publish it to the topic once
   per second, printing a `Delivered:` line via the `delivery_report` callback.

The event shape (`generate_events()`):

```python
{
  "user_id": fake.uuid4(),                       # unique per event (note: see §6)
  "event_type": fake.random_element([...]),      # login | click_nav | purchase | logout
  "product_id": fake.pyint(1000, 9999),
  "amount": fake.random_number(digits=4),
  "event_timestamp": fake.iso8601()              # RANDOM date 1970..2026  <-- key gotcha
}
```

`Ctrl+C` triggers `producer.flush()` and a clean stop.

### 3.6 `producer/requirements.txt`

Host-side Python deps: `confluent-kafka` (Kafka client), `faker` (fake data),
`python-dotenv` (read `.env`). `confluent-kafka` is pinned `>=2.6.0` so that pip installs a
prebuilt Python-3.13 Windows wheel instead of trying (and failing) to compile from source.

### 3.7 `spark_jobs/bronze_ingest.py` — RAW ingestion (Bronze)

- Builds a `SparkSession` configured for Delta + the MinIO `s3a://` filesystem.
- `readStream.format("kafka")` subscribed to `ecommerce-events`, `startingOffsets=earliest`.
- Writes the **untransformed** Kafka columns straight to
  `s3a://lakehouse/bronze/ecommerce-events` in Delta format, `append` mode.
- **Checkpoint:** `s3a://lakehouse/checkpoints/bronze` tracks which Kafka offsets have been
  committed → gives exactly-once / fault tolerance and resume-after-restart.
- **Trigger:** `processingTime="10 seconds"` → a new micro-batch every 10s.

### 3.8 `spark_jobs/silver_clean.py` — clean & structure (Silver)

- `ensure_silver_table()` (added to make startup robust): if the Silver table doesn't have a
  schema yet, it writes a zero-row commit with the explicit schema. This guarantees Gold's
  `readStream` can attach immediately instead of crashing with `DELTA_SCHEMA_NOT_SET`.
- `read_data()`: `readStream` from the **Bronze** Delta table; parses the raw `value` bytes
  with `from_json(..., schema)` and flattens to columns via `select("data.*")`.
- `validate_data()`:
  - casts `event_timestamp` (string) → real `timestamp`,
  - filters out null `user_id` / null `event_timestamp`,
  - applies a **5-minute watermark** on `event_timestamp`,
  - `dropDuplicates([...])` for deduplication,
  - adds `processed_at = current_timestamp()` for observability.
- Writes to `s3a://lakehouse/silver/ecommerce-events`, `append`, checkpoint
  `.../checkpoints/silver`, trigger `30 seconds`.

### 3.9 `spark_jobs/gold_aggregate.py` — KPIs (Gold)

- `readStream` from the **Silver** Delta table.
- `aggregated_data()`:
  - **10-minute watermark** on `event_timestamp` (allows late data up to 10 min),
  - groups by `window("event_timestamp", "5 minutes")` → **5-minute tumbling windows**,
  - computes KPIs: `total_events`, `unique_users` (approx distinct), `total_revenue`
    (sum of purchase amounts), `purchase_count`, `avg_order_value`, and a derived
    `conversion_rate = purchase_count / total_events`,
  - explodes the `window` struct into `window_start` / `window_end`.
- Writes to `s3a://lakehouse/gold/kpi-metrices`, **`complete`** output mode (the whole
  aggregate table is rewritten each batch), checkpoint `.../checkpoints/gold`, trigger
  `60 seconds`.
- The `while query.isActive: print(query.lastProgress)` loop is why this terminal prints big
  JSON progress dictionaries every ~30s (see §4).

### 3.10 `spark_jobs/gold_test.py` — verification (one-shot batch)

Not a stream. A plain batch job that reads each Delta table once and prints its row count.
It was made resilient so a not-yet-created layer prints a friendly message instead of
crashing. Example output from your run:

```
bronze_layer: 2506 rows
silver_layer: 342 rows
gold_layer: 342 rows
```

### 3.11 `README.md`

The quick-start runbook: prerequisites, `docker compose up`, start the producer, submit the
three jobs in order, verify, shut down.

### 3.12 Stray nested folder (cleanup note)

There is an accidental duplicate directory `E-Commerce-Events-Real-Time-Data-Pipeline/`
*inside* the project containing a stray `.env` and `producer/requirements.txt`. It is unused
and safe to delete to avoid confusion.

---

## 4. Terminal-by-terminal reference (your live run)

You typically have **six** terminals open. Here is what each one is doing, plus how to read
its output.

### Terminal A — Infrastructure (`docker compose up -d --build`)
Brings up Kafka, MinIO, the bucket initializer, and Spark master+worker. After it finishes,
`docker compose ps` should show `kafka` and `minio-lakehouse` as `(healthy)`, the two Spark
containers `Up`, and `minio-init` `Exited (0)`. This terminal is then free; the services
keep running in the background.

### Terminal B — Producer (`python producer/producer.py`)
The data source. Streams ~1 event/sec into Kafka. Healthy output looks like:

```
Delivered:{"user_id": "af537ad1-...", "event_type": "purchase", "product_id": 8990, "amount": 2179, "event_timestamp": "2016-01-30T19:19:29"}
```

If this terminal isn't running, Bronze stops growing and the whole pipeline goes idle.

### Terminal C — Bronze (`spark-submit .../bronze_ingest.py`)
Long-running stream: Kafka → Bronze Delta. Key log lines and what they mean:

- `Granted executor ID ... with 1 core(s), 512.0 MiB RAM` → the job got its resources
  (the §5 fix working).
- `WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is
  10000 milliseconds, but spent 26676 milliseconds` → a batch took 26.7s but the trigger
  wanted 10s. This is **backpressure**: writing Delta to MinIO + checkpointing is slow on a
  small local box. It's a warning, not an error; Spark just runs the next batch immediately.

### Terminal D — Silver (`spark-submit .../silver_clean.py`)
Long-running stream: Bronze → Silver. You'll see `Bootstrapping empty Silver table...` on
first run (the schema bootstrap), then batch logs. In your run the first batch was very slow
(`trigger interval is 30000 ms, but spent 257695 ms`) because it processed the Bronze
backlog (2000+ rows) plus stateful dedup on a 512 MB executor. This is why **Silver lags
Bronze** — at the snapshot, Bronze had 2506 rows but Silver had only caught up to 342.

### Terminal E — Gold (`spark-submit .../gold_aggregate.py`)
Long-running stream: Silver → Gold. Because of the `print(query.lastProgress)` loop, this
terminal prints a JSON progress report each cycle. How to read it:

```jsonc
{
  "batchId": 1,
  "numInputRows": 0,                       // rows read this batch
  "eventTime": { "watermark": "2026-05-18T06:34:50.000Z" },  // current watermark
  "stateOperators": [
    { "operatorName": "stateStoreSave", "numRowsTotal": 342, ... }  // windows held in state
  ],
  "sources": [ { "description": "DeltaSource[s3a://lakehouse/silver/ecommerce-events]" } ],
  "sink":    { "description": "DeltaSink[s3a://lakehouse/gold/kpi-metrices]" }
}
```

- `watermark: 2026-05-18...` = (max event time seen) − 10 minutes. It's that recent because
  the random generator occasionally emits near-present dates (e.g. `2026-04-23`).
- `numRowsTotal: 342` = number of distinct 5-minute windows currently in the state store
  (≈ one per event, due to the random-timestamp issue in §6).

### Terminal F — Verification (`spark-submit .../gold_test.py`)
One-shot. Run it whenever you want a snapshot of row counts, then it exits. Your run printed
`bronze_layer: 2506`, `silver_layer: 342`, `gold_layer: 342`.

> Earlier `Py4JNetworkError` / `An error occurred while calling o32.sc` tracebacks in the
> Bronze/Silver terminals are simply from a **previous run being Ctrl+C-ed** (the Python
> driver tearing down its connection to the JVM). They are not bugs in the current run.

---

## 5. Resource model (why 512 MB matters)

The single worker advertises **4 cores / 2 GB**. Every running application reserves one
executor. With the default 1 GB executor, three concurrent jobs need 3 GB > 2 GB, so the
third would hang on `Initial job has not accepted any resources`.

The jobs were therefore pinned to `spark.executor.memory=512m` and `spark.cores.max=1`:

| Job          | Cores | Memory |
|--------------|-------|--------|
| Bronze       | 1     | 512 MB |
| Silver       | 1     | 512 MB |
| Gold         | 1     | 512 MB |
| **Subtotal** | **3** | **1536 MB** |
| (gold_test, when run) | 1 | 512 MB |

This fits inside 2 GB. For real headroom, raise Docker Desktop RAM and
`SPARK_WORKER_MEMORY` in `docker-compose.yaml` instead.

---

## 6. Known issue & recommended fix: random event timestamps

`producer.py` uses `fake.iso8601()`, producing timestamps spread across decades. Because
Silver/Gold key off *event time*:

- Gold's 5-minute windows almost never collect more than one event → aggregates are
  meaningless (one row per window).
- This is why `gold_layer` row count ≈ `silver_layer` row count in your run.

**Fix** — emit current time so windows actually group:

```python
from datetime import datetime, timezone
# ...
"event_timestamp": datetime.now(timezone.utc).isoformat()
```

After this change, restart the producer; Gold windows will start showing real
`total_events`, `unique_users`, `total_revenue`, and `conversion_rate` values.

---

## 7. Where the data physically lives (MinIO)

Browse `http://localhost:9001` (login `minioadmin` / `minioadmin`), bucket `lakehouse`:

```
lakehouse/
├── bronze/ecommerce-events/      ← raw Kafka envelopes (Delta)
├── silver/ecommerce-events/      ← cleaned, typed, deduped (Delta)
├── gold/kpi-metrices/            ← windowed KPIs (Delta)
└── checkpoints/
    ├── bronze/   ├── silver/   └── gold/   ← stream offsets & state (do not delete)
```

Each Delta folder contains Parquet data files plus a `_delta_log/` directory of JSON commit
files that make up the table's transaction log.

---

## 8. Operating cheat-sheet

```powershell
# Start everything (from project root)
docker compose up -d --build
python producer/producer.py            # Terminal B

# Submit streams in order, each in its own terminal:
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/bronze_ingest.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/silver_clean.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_aggregate.py

# Verify row counts any time:
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_test.py

# Stop
docker compose down        # keep data
docker compose down -v     # wipe Kafka + MinIO volumes
```

Useful UIs: Spark master `http://localhost:8080`, MinIO console `http://localhost:9001`.
