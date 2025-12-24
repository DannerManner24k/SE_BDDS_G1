import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime # <--- Added this

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
KAFKA_BOOTSTRAP_SERVER = 'localhost:9094'
KAFKA_TOPIC = 'weather.live.stream'

print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVER}...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_live_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 55.4,
        "longitude": 10.4,
        "current": "temperature_2m,wind_speed_10m"
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        current = data.get('current', {})
        
        # --- THE FIX ---
        # We use the SYSTEM time, not the API time, to force the graph to move.
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            "time": current_time, # <--- Dynamic Time
            "temperature": current.get("temperature_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "source": "open-meteo-live"
        }
        return payload

    except Exception as e:
        print(f"❌ Fetch Error: {e}")
        return None

# ---------------------------------------------------------
# THE LIVE LOOP
# ---------------------------------------------------------
print("🌤️ Starting Weather Station (Simulated Live Clock)...")

while True:
    weather = get_live_weather()
    
    if weather:
        producer.send(KAFKA_TOPIC, value=weather)
        producer.flush()
        print(f"🚀 Sent Weather: {weather['temperature']}°C at {weather['time']}")
    else:
        print("⚠️ No weather data.")
    
    time.sleep(5) # Faster updates (every 5s) to make the graph smooth