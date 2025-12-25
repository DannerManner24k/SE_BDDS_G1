from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, to_timestamp, hour, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor  # <--- The Challenger
from pyspark.ml import Pipeline

print("Starting Training: GBT Challenger Model...")

spark = SparkSession.builder \
    .appName("TrainGBT") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. LOAD DATA (Same as before)
df_weather = spark.read.parquet("s3a://silver/weather_15min/")
df_prices = spark.read.json("s3a://bronze/history/prices*.json") \
    .withColumn("price_time", to_timestamp("HourUTC", "yyyy-MM-dd'T'HH:mm:ss")) \
    .select(col("price_time"), col("SpotPriceEUR").alias("label"))

# 2. JOIN
df_weather = df_weather.withColumn("join_hour", date_trunc("hour", col("time_15min")))
training_data = df_weather.join(
    df_prices, 
    df_weather.join_hour == df_prices.price_time
).withColumn("hour_of_day", hour("time_15min")) \
 .withColumn("day_of_week", dayofweek("time_15min")) \
 .select("temperature", "wind_speed", "hour_of_day", "day_of_week", "label")

# 3. TRAIN GBT (Boosting)
assembler = VectorAssembler(
    inputCols=["temperature", "wind_speed", "hour_of_day", "day_of_week"],
    outputCol=("features")
)

# GBT Configuration (Shallow trees, many iterations)
gbt = GBTRegressor(featuresCol="features", labelCol="label", maxIter=20, maxDepth=5)
pipeline = Pipeline(stages=[assembler, gbt])

model = pipeline.fit(training_data)

# 4. SAVE TO SEPARATE FOLDER
model.write().overwrite().save("s3a://gold/price_prediction_model_gbt")

print("✅ Challenger Model (GBT) Saved to Gold Layer.")
spark.stop()