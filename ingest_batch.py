import requests
import json
import pandas as pd
from minio import Minio
from io import BytesIO

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio_admin"
MINIO_SECRET_KEY = "minio_password"
BUCKET_NAME = "bronze"

# We fetch 2024 data for the "DK1" (Western Denmark) grid area
START_DATE = "2024-01-01"
END_DATE = "2024-01-31" # Short range for the first test run

# ---------------------------------------------------------
# 1. CONNECT TO THE VAULT (MinIO)
# ---------------------------------------------------------
print(f"🔌 Connecting to MinIO at {MINIO_ENDPOINT}...")
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # We are using HTTP, not HTTPS
)

# ---------------------------------------------------------
# 2. FETCH MARKET DATA (Energi Data Service)
# Source: Elspotprices (Hourly)
# ---------------------------------------------------------
print("💰 Fetching Price Data (EDS)...")
eds_url = "https://api.energidataservice.dk/dataset/Elspotprices"
params = {
    "start": START_DATE,
    "end": END_DATE,
    "filter": '{"PriceArea":["DK1"]}',
    "columns": "HourUTC,SpotPriceEUR",
    "sort": "HourUTC ASC"
}

response = requests.get(eds_url, params=params)
price_data = response.json().get('records', [])

# Save to MinIO as a JSON file
price_json = json.dumps(price_data).encode('utf-8')
price_path = f"history/prices_{START_DATE}_to_{END_DATE}.json"

client.put_object(
    BUCKET_NAME,
    price_path,
    data=BytesIO(price_json),
    length=len(price_json),
    content_type="application/json"
)
print(f"✅ Saved Price Data to minio://{BUCKET_NAME}/{price_path}")

# ---------------------------------------------------------
# 3. FETCH WEATHER DATA (Open-Meteo)
# Source: Historical Weather API
# Location: Odense, Denmark (Lat 55.4, Lon 10.4)
# ---------------------------------------------------------
print("gen☁️ Fetching Weather Data (Open-Meteo)...")
weather_url = "https://archive-api.open-meteo.com/v1/archive"
weather_params = {
    "latitude": 55.4,
    "longitude": 10.4,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "hourly": "temperature_2m,windspeed_10m"
}

response = requests.get(weather_url, params=weather_params)
weather_data = response.json()

# Save to MinIO as a JSON file
weather_json = json.dumps(weather_data).encode('utf-8')
weather_path = f"history/weather_{START_DATE}_to_{END_DATE}.json"

client.put_object(
    BUCKET_NAME,
    weather_path,
    data=BytesIO(weather_json),
    length=len(weather_json),
    content_type="application/json"
)
print(f"✅ Saved Weather Data to minio://{BUCKET_NAME}/{weather_path}")

print("\n🎉 Phase 2 (Batch) Complete: Raw data is in the Vault.")