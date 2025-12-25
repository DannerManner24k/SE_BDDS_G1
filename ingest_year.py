import os
import requests
import json
import time
from datetime import datetime, timedelta
from minio import Minio
from io import BytesIO

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minio_admin",
    secret_key="minio_password",
    secure=False
)
BUCKET_BRONZE = "bronze"

# We will fetch all of 2023
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2023, 12, 31)

def upload_to_minio(data, filename, folder):
    """Uploads JSON data to MinIO Bronze bucket."""
    json_bytes = json.dumps(data).encode('utf-8')
    buffer = BytesIO(json_bytes)
    path = f"{folder}/{filename}"
    
    MINIO_CLIENT.put_object(
        BUCKET_BRONZE, path, buffer, len(json_bytes), content_type="application/json"
    )
    print(f"✅ Uploaded: {path}")

def fetch_weather_batch(start, end):
    """Fetches Open-Meteo history for a specific date range."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 55.4,
        "longitude": 10.4,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,wind_speed_10m"
    }
    resp = requests.get(url, params=params)
    return resp.json()

def fetch_price_batch(start, end):
    """Fetches Energi Data Service prices for a specific date range."""
    url = "https://api.energidataservice.dk/dataset/Elspotprices"
    params = {
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
        "filter": '{"PriceArea":"DK1"}',
        "columns": "HourUTC,SpotPriceEUR",
        "limit": 0 # No limit
    }
    resp = requests.get(url, params=params)
    return resp.json().get('records', [])

# ---------------------------------------------------------
# EXECUTION LOOP
# ---------------------------------------------------------
print("🚀 Starting Full Year Ingestion (2023)...")

current_date = START_DATE
while current_date <= END_DATE:
    # We fetch in 1-month chunks
    next_month = current_date + timedelta(days=32)
    chunk_end = next_month.replace(day=1) - timedelta(days=1)
    
    if chunk_end > END_DATE:
        chunk_end = END_DATE
        
    print(f"📥 Fetching: {current_date.date()} -> {chunk_end.date()}")
    
    # 1. Weather
    weather_data = fetch_weather_batch(current_date, chunk_end)
    upload_to_minio(weather_data, f"weather_{current_date.strftime('%Y_%m')}.json", "history")
    
    # 2. Prices
    price_data = fetch_price_batch(current_date, chunk_end)
    upload_to_minio(price_data, f"prices_{current_date.strftime('%Y_%m')}.json", "history")
    
    # Move to next month
    current_date = chunk_end + timedelta(days=1)
    time.sleep(1) # Be nice to the API

print("Full year ingestion complete.")