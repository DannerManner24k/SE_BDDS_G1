from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, to_timestamp, hour, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

print("Starting Model Training V2 (Smart Time Features)...")

spark = SparkSession.builder \
    .appName("GoldLayerTraining") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. LOAD DATA
df_weather = spark.read.parquet("s3a://silver/weather_15min/")
df_prices = spark.read.json("s3a://bronze/history/prices*.json") \
    .withColumn("price_time", to_timestamp("HourUTC", "yyyy-MM-dd'T'HH:mm:ss")) \
    .select(col("price_time"), col("SpotPriceEUR").alias("label"))

# 2. JOIN & FEATURE ENGINEERING
df_weather = df_weather.withColumn("join_hour", date_trunc("hour", col("time_15min")))

training_data = df_weather.join(
    df_prices, 
    df_weather.join_hour == df_prices.price_time
).select(
    "temperature",
    "wind_speed",
    "label",
    "time_15min"
)

# --- NEW FEATURES ---
# We extract Hour (0-23) and DayOfWeek (1-7)
# This teaches the model that "Sunday" and "Night" are cheaper.
training_data = training_data \
    .withColumn("hour_of_day", hour("time_15min")) \
    .withColumn("day_of_week", dayofweek("time_15min"))

print("Feature Columns: temperature, wind_speed, hour_of_day, day_of_week")

# 3. TRAIN
assembler = VectorAssembler(
    inputCols=["temperature", "wind_speed", "hour_of_day", "day_of_week"],
    outputCol=("features")
)

rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=20) # More trees for better accuracy
pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(training_data)

# 4. SAVE
# We save to a NEW path for V2
model.write().overwrite().save("s3a://gold/price_prediction_model_v2")

print("✅ Model V2 (7-Day Capable) Trained & Saved.")
spark.stop()