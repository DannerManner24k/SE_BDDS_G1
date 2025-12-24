# Full-Stack Data Lakehouse

This is a complete implementation of a **Real-Time Energy Price Forecasting System**. It uses a Data Lakehouse architecture to ingest weather data, process it with Spark, train a Machine Learning model, and serve live predictions to a Trader's Dashboard.

## Table of Contents

1. [Prerequisites]
2. [Infrastructure Setup (Docker)]
3. [The Vault Setup (MinIO)]
4. [Local Environment Setup]
5. [Deployment (Copying Scripts)]
6. [Execution Phase 1: The Backend]
7. [Execution Phase 2: The Live Stream]
8. [Execution Phase 3: The Dashboard]
9. [Shutdown & Troubleshooting]

---

## 1. Prerequisites

* **Docker Desktop** installed and running.
* **Python 3.10+** installed on your host machine.
* **Git Bash / PowerShell** (Windows) or Terminal (Mac/Linux).

---

## 2. Infrastructure Setup (Docker)

We use a custom `docker-compose.yaml` that networks Spark, Kafka, and MinIO together. It also exposes a special "External Port" (9094) for Kafka so your Windows scripts can talk to it.

**Action:** Create a file named `docker-compose.yaml` in your root folder and paste this exact config:

```yaml
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: "minio_admin"
      MINIO_ROOT_PASSWORD: "minio_password"
    command: server /data --console-address ":9001"
    volumes:
      - ./minio_data:/data

  zookeeper:
    image: bitnamilegacy/zookeeper:3.9.0
    container_name: zookeeper
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    ports:
      - "2181:2181"

  kafka:
    image: bitnamilegacy/kafka:3.5.1
    container_name: kafka
    ports:
      - "9092:9092" # Internal Docker communication
      - "9094:9094" # External Windows communication
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_LISTENERS=CLIENT://:9092,EXTERNAL://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://kafka:9092,EXTERNAL://localhost:9094
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=CLIENT
    depends_on:
      - zookeeper

  spark-master:
    image: bitnamilegacy/spark:3.5.0
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8080:8080"
      - "7077:7077"
      - "4040:4040"

  spark-worker:
    image: bitnamilegacy/spark:3.5.0
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    depends_on:
      - spark-master

```

**Action:** Start the cluster:

```powershell
docker-compose up -d

```

**Action:** Install `numpy` inside the Spark containers (Required for ML, deleted on restart):

```powershell
docker exec --user root spark-master pip install numpy
docker exec --user root spark-worker pip install numpy

```

---

## 3. The Vault Setup (MinIO)

Spark will fail if the storage buckets do not exist. You must create them manually.

1. Open your browser to: **`http://localhost:9001`**
2. Login with:
* User: `minio_admin`
* Password: `minio_password`


3. Click **Buckets** in the left sidebar.
4. Click **Create Bucket** and create these three exactly (all lowercase):
* `bronze`
* `silver`
* `gold`


5. **Upload Data:** Go into the `bronze` bucket and upload your raw historical JSON files (e.g., `weather_2024.json`, `prices_2024.json`). *Note: Without this initial data, the training script will have nothing to learn from.*

---

## 4. Local Environment Setup

Your local machine runs the Dashboard and the Data Producers.

**Action:** Open a terminal and install dependencies:

```powershell
pip install streamlit kafka-python requests pandas altair matplotlib

```

---

## 5. Deployment (Copying Scripts)

Your Python logic needs to be inside the Spark container to run.

**Action:** Run these commands to copy your scripts from Windows to Docker:

```powershell
# 1. ETL Script (Refining Data)
docker cp process_silver.py spark-master:/opt/bitnami/spark/

# 2. Training Script (Creating the Model)
docker cp train_model.py spark-master:/opt/bitnami/spark/

# 3. Inference Script (The Real-Time Pilot)
docker cp stream_inference.py spark-master:/opt/bitnami/spark/

```

---

## 6. Execution Phase 1: The Backend

We now build the brain of the system. Run these commands sequentially in your terminal.

**Step 6.1: Run ETL (Bronze -> Silver)**
Refines raw JSON into high-performance Parquet files.

```powershell
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 process_silver.py

```

**Step 6.2: Train the Model (Silver -> Gold)**
Teaches the Random Forest model using historical data.

```powershell
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 train_model.py

```

*Wait for the success message:* `✅ Model V2 (7-Day Capable) Trained & Saved.`

---

## 7. Execution Phase 2: The Live Stream

**CRITICAL ORDER OF OPERATIONS:** You *must* start the Python producers first to create the Kafka topics. If you start Spark first, it will crash with `UnknownTopicOrPartitionException`.

**Step 7.1: Start the Producers**
Open a **NEW Terminal (Terminal A)** and run:

1. **Initialize 7-Day Forecast:** (Sends a burst of future data)
```powershell
python producer_forecast.py

```


2. **Start Live Weather Feed:** (Keeps running forever)
```powershell
python producer_weather.py

```


*Leave this running! It simulates the passage of time.*

**Step 7.2: Start the Spark Pilot**
Go back to your **Main Terminal (Terminal B)**.

If you have run this before, clear the old checkpoints to prevent errors:

```powershell
docker exec spark-master rm -rf /tmp/checkpoints

```

Launch the streaming engine:

```powershell
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 stream_inference.py

```

*Note:* This command will appear to "hang" or freeze. **This is good.** It is running silently in the background, processing data and sending it to Kafka.

---

## 8. Execution Phase 3: The Dashboard

Now we visualize the results.

Open a **Third Terminal (Terminal C)** and run:

```powershell
streamlit run dashboard.py

```

### The "Demo Sequence"

To show off the system effectively:

1. Open the Dashboard link (`http://localhost:8501`).
2. You will see the **Top Graph** moving (Live Weather).
3. The **Bottom Graph** might be empty initially.
4. Run `python producer_forecast.py` again in a separate window.
5. Watch the Dashboard: The bottom graph will suddenly animate and draw the orange forecast curve for the next 7 days.

---

## 9. Shutdown & Troubleshooting

### Clean Shutdown

To stop everything and free up RAM:

```powershell
docker-compose down

```

*Warning:* This deletes the Kafka topics and Spark internal files (but MinIO data persists if volume mapping works, otherwise MinIO is wiped too).

### Common Errors

* **`ModuleNotFoundError: No module named 'numpy'`**
* **Fix:** You restarted the container. Run the `pip install numpy` commands in Section 2 again.


* **`FileAlreadyExistsException` / `SparkConcurrentModificationException**`
* **Fix:** Your previous run left a locked checkpoint. Run: `docker exec spark-master rm -rf /tmp/checkpoints`.


* **`UnknownTopicOrPartitionException`**
* **Fix:** Kafka is empty. Run `python producer_forecast.py` once to create the topics, *then* restart the Spark command.


* **Dashboard graph isn't moving**
* **Fix:** Ensure `producer_weather.py` is running. It provides the "heartbeat" of the system.