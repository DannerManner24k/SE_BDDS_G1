import time
import json
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = 'localhost:9094'
KAFKA_TOPIC = 'weather.live.stream' # We reuse the same pipe!

print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVER}...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_and_send_forecast():
    print("🔮 Fetching 7-Day Forecast from Open-Meteo...")
    
    # We ask for 7 days of hourly data
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 55.4,
        "longitude": 10.4,
        "hourly": "temperature_2m,wind_speed_10m",
        "forecast_days": 7
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        hourly = data.get('hourly', {})
        
        times = hourly.get('time', [])
        temps = hourly.get('temperature_2m', [])
        winds = hourly.get('wind_speed_10m', [])
        
        count = 0
        # Iterate through all 168 hours (7 days * 24 hours)
        for i in range(len(times)):
            payload = {
                "time": times[i], # Actual future time (e.g. 2025-12-30T10:00)
                "temperature": temps[i],
                "wind_speed": winds[i],
                "source": "7-day-forecast"
            }
            
            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=payload)
            count += 1
            
            # Small throttle to not overwhelm the dashboard animation
            time.sleep(0.05) 
            
        producer.flush()
        print(f"✅ Successfully pushed {count} forecast hours to the system.")

    except Exception as e:
        print(f"❌ Error: {e}")

# Run once
fetch_and_send_forecast()