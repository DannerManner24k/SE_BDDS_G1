from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, hour, dayofweek, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel

print("🚀 Starting Pilot V2 (7-Day Forecast Mode)...")

spark = SparkSession.builder \
    .appName("LiveInferencePilot") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# LOAD NEW V2 MODEL
model = PipelineModel.load("s3a://gold/price_prediction_model_v2")

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

# PARSE & PREPARE FEATURES
weather_df = raw_stream.select(
    from_json(col("value").cast("string"), weather_schema).alias("data")
).select(
    col("data.time"),
    col("data.temperature"),
    col("data.wind_speed"),
    # We must convert string time to timestamp to extract features
    to_timestamp(col("data.time")).alias("ts")
)

# EXTRACT NEW FEATURES
weather_df = weather_df \
    .withColumn("hour_of_day", hour("ts")) \
    .withColumn("day_of_week", dayofweek("ts"))

# PREDICT
predictions = model.transform(weather_df)

output_df = predictions.select(
    to_json(struct(
        col("time"),
        col("temperature"),
        col("wind_speed"),
        col("prediction").alias("predicted_price")
    )).alias("value")
)

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "model.predictions") \
    .option("checkpointLocation", "/tmp/checkpoints/predictions_v2") \
    .start()

query.awaitTermination()