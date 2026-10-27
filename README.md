# Crypto Pulse Pipeline

Real-Time Crypto Streaming & Anomaly Detection Pipeline for real-time cryptocurrency price ingestion, real-time Machine Learning anomaly detection, transaction-safe storage, and automated orchestration.

---

## System architecture

The pipeline streams live price ticks from Binance via WebSockets, ingests them into Kafka, processes the stream in real time with Apache Spark, runs Machine Learning anomaly detection on micro-batches using Scikit-learn, and persists results into both Delta Lake and PostgreSQL. Apache Airflow orchestrates daily data maintenance tasks.

---

## Tech Stack

* **Ingestion:** Binance WebSocket API, `kafka-python`, `websockets`
* **Message Broker:** Apache Kafka 3.7 (KRaft mode)
* **Stream Processing & ML:** Apache Spark 3.5.1 (Structured Streaming), PySpark, Pandas UDF, Scikit-learn (`IsolationForest`)
* **Storage Layer:** Delta Lake 3.1.0, PostgreSQL 17
* **Orchestration & DataOps:** Apache Airflow 2.8.1 (LocalExecutor)
* **Infrastructure:** Docker & Docker Compose

---

## Repository Structure

```text
crypto-pulse-pipeline/
├── airflow/
│   └── dags/
│       └── crypto_maintenance_dag.py
├── src/
│   ├── producer/
│   │   └── binance_producer.py
│   ├── processor/
│   │   └── spark_stream.py
│   └── storage/
│       └── db_client.py
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Key Features

* **KRaft-based Kafka Cluster:** Operates for streamlined local deployments.
* **Real-Time Inference via Pandas UDF:** Executes `IsolationForest` anomaly detection directly inside PySpark using Pandas functions.
* **ACID Storage Layer:** Guarantees transactional integrity and time-travel capability using Delta Lake.
* **Automated DataOps:** Airflow DAG executes daily health checks on PostgreSQL and performs `OPTIMIZE` and `VACUUM` operations on Delta Lake tables.

---

## Quickstart Guide

### Prerequisites
* Docker Engine & Docker Compose
* Python 3.10+

### 1. Start Infrastructure
Launch all core services:
```bash
docker compose up -d
```

### 2. Verify Services
* **Kafka UI:** [http://localhost:8080](http://localhost:8080)
* **Airflow Webserver:** [http://localhost:8090](http://localhost:8090)
* **PostgreSQL (Crypto Database):** `localhost:54332` (`CryptoDb`)

### 3. Run Pipeline Components
In separate terminal sessions, start the producer and stream processor:

**Start Producer:**
```bash
python src/producer/binance_producer.py
```

**Start Spark Stream Processor:**
```bash
python src/processor/spark_stream.py
```

---

## Database Schema

### `crypto_anomalies` Table (PostgreSQL)
| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | Auto-incremented anomaly identifier |
| `symbol` | `TEXT NOT NULL` | Ticker symbol |
| `price` | `REAL NOT NULL` | Trade price at detection time |
| `volume` | `REAL` | Trade volume |
| `detected_at` | `TIMESTAMP NOT NULL` | Timestamp of anomaly detection |