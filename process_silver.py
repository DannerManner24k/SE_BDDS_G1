from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, expr, to_timestamp

# ---------------------------------------------------------
# 1. INITIALIZE SPARK
# ---------------------------------------------------------
print("⚙️ Starting Spark Refinery (V3)...")

spark = SparkSession.builder \
    .appName("SilverLayerRefinery") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# ---------------------------------------------------------
# 2. READ BRONZE WEATHER
# ---------------------------------------------------------
print("📥 Reading Bronze Weather Data...")
df_raw = spark.read.json("s3a://bronze/history/weather*.json")

# ---------------------------------------------------------
# 3. FLATTEN THE ARRAYS (The "Unzip" Step)
# ---------------------------------------------------------
print("🔨 Flattening Open-Meteo Arrays...")

# Step 1: Zip the arrays together so indices match
# Step 2: Explode them into rows
# Step 3: Select by NAME (time, temperature_2m) not index (0, 1)

df_flattened = df_raw.select(
    explode(arrays_zip(
        col("hourly.time"), 
        col("hourly.temperature_2m"), 
        col("hourly.windspeed_10m")
    )).alias("data")
).select(
    col("data.time").alias("time_str"),             # <--- CHANGED THIS
    col("data.temperature_2m").alias("temperature"), # <--- AND THIS
    col("data.windspeed_10m").alias("wind_speed")    # <--- AND THIS
)

# Convert string time to timestamp
df_flattened = df_flattened.withColumn("timestamp", to_timestamp("time_str", "yyyy-MM-dd'T'HH:mm"))

# ---------------------------------------------------------
# 4. UPSAMPLING (1 Hour -> 15 Mins)
# ---------------------------------------------------------
print("🔥 Refining: Upsampling Hourly to 15-Minute...")

df_flattened.createOrReplaceTempView("weather_hourly")

refined_df = spark.sql("""
    SELECT 
        timestamp as base_time,
        temperature,
        wind_speed,
        explode(array(0, 15, 30, 45)) as minute_offset
    FROM weather_hourly
""")

refined_df = refined_df.withColumn(
    "time_15min", 
    expr("base_time + make_interval(0, 0, 0, 0, 0, minute_offset, 0)")
).drop("minute_offset", "base_time")

refined_df.printSchema()

# ---------------------------------------------------------
# 5. WRITE TO SILVER
# ---------------------------------------------------------
print("💾 Writing to Silver Layer (Parquet)...")

refined_df.write \
    .mode("overwrite") \
    .parquet("s3a://silver/weather_15min/")

print("✅ Phase 3 Complete: Weather data is refined.")
spark.stop()