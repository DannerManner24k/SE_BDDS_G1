from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, hour, dayofweek, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel

print("🚀 Starting Dual-Core Pilot (RF vs GBT)...")

spark = SparkSession.builder \
    .appName("DualInferencePilot") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. LOAD BOTH MODELS
print("🧠 Loading Champion (Random Forest)...")
model_rf = PipelineModel.load("s3a://gold/price_prediction_model_v2")

print("🧠 Loading Challenger (GBT)...")
model_gbt = PipelineModel.load("s3a://gold/price_prediction_model_gbt")

# 2. READ STREAM
weather_schema = StructType([
    StructField("time", StringType()),
    StructField("temperature", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("source", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather.live.stream") \
    .load()

# 3. PREPARE DATA
weather_df = raw_stream.select(
    from_json(col("value").cast("string"), weather_schema).alias("data")
).select(
    col("data.time"),
    col("data.temperature"),
    col("data.wind_speed"),
    to_timestamp(col("data.time")).alias("ts")
).withColumn("hour_of_day", hour("ts")) \
 .withColumn("day_of_week", dayofweek("ts"))

# 4. PREDICT SEQUENTIALLY (WITH CLEANUP)

# --- PASS 1: Random Forest ---
# This adds 'features' and 'prediction' columns
df_rf = model_rf.transform(weather_df) \
    .withColumnRenamed("prediction", "price_rf")

# CRITICAL FIX: Drop 'features' so the next model can recreate it without crashing
df_clean_for_gbt = df_rf.drop("features")

# --- PASS 2: GBT ---
# Now GBT can safely create its own 'features' column
df_gbt = model_gbt.transform(df_clean_for_gbt) \
    .withColumnRenamed("prediction", "price_gbt")

# Final Cleanup: Drop 'features' again before sending to Kafka (save bandwidth)
final_df = df_gbt.drop("features")

# 5. OUTPUT
output_df = final_df.select(
    to_json(struct(
        col("time"),
        col("temperature"),
        col("wind_speed"),
        col("price_rf"),
        col("price_gbt")
    )).alias("value")
)

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "model.predictions") \
    .option("checkpointLocation", "/tmp/checkpoints/predictions_dual") \
    .start()

query.awaitTermination()