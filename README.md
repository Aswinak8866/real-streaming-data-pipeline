# Real-Time Streaming Data Pipeline

A real-time e-commerce event pipeline: a Python producer publishes events to **Kafka**, **Spark Structured Streaming** consumes and parses them, and the results are appended to an **Apache Iceberg** table stored on **HDFS**.

## Architecture
```
producer.py ──▶ Kafka (ecommerce-events) ──▶ Spark Structured Streaming ──▶ Iceberg table on HDFS
 (1 event/sec)      localhost:9092             parse JSON, append            local.ecommerce.events
```

## Event schema
| Field | Type | Example |
|---|---|---|
| event_id | string | evt_483920 |
| user_id | string | 417 |
| event_type | string | view_product / add_to_cart / purchase / payment |
| product_id | string | P512 |
| amount | double | 2499.50 |
| timestamp | string (ISO-8601) | 2026-10-23T12:30:00 |

## Tech stack
Python, Apache Kafka, Apache Spark 3.5 (PySpark Structured Streaming), Apache Iceberg, Hadoop HDFS

## Prerequisites
- Python 3.10+ (tested on 3.10.12), Java 11 or 17
- Spark 3.5.x (Scala 2.12) with `spark-submit` on your PATH
- Hadoop 3.3.x with HDFS running at `hdfs://localhost:9000`
- Kafka broker on `localhost:9092` (for example via Docker)

## Setup
```bash
git clone https://github.com/Aswinak8866/real-time-streaming-pipeline.git
cd real-time-streaming-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt   # installs kafka-python for the producer
```

## Run
1. Start HDFS:
   ```bash
   $HADOOP_HOME/sbin/start-dfs.sh
   ```
2. Start Kafka (Docker example):
   ```bash
   docker run -d --name kafka -p 9092:9092 apache/kafka:latest
   ```
3. Check the Iceberg connection and create the table:
   ```bash
   spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 spark/iceberg_test.py
   ```
4. Start the streaming job (leave it running):
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 spark/streaming.py
   ```
5. In another terminal, start the producer:
   ```bash
   python producer/producer.py
   ```
6. Query the data by running `iceberg_test.py` again. It prints the row count.

Adjust the package versions to match your Spark version.

## Project structure
```
.
├── producer/producer.py    # generates fake e-commerce events into Kafka
├── spark/
│   ├── streaming.py        # Kafka -> Spark Structured Streaming -> Iceberg
│   └── iceberg_test.py     # creates and checks the Iceberg table
├── requirements.txt
└── README.md
```

## Configuration
Hosts and paths (`localhost:9092`, `hdfs://localhost:9000`, `/user/vboxuser/...`) are constants at the top of each script. Change them for your environment.

## Roadmap
Docker Compose setup, Kubernetes deployment, tests, and dashboards over the Iceberg table.
