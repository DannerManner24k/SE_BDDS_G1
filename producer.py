import time
import json
import requests
from kafka import KafkaProducer

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
KAFKA_BOOTSTRAP_SERVER = 'localhost:9094'
KAFKA_TOPIC = 'energy.live.stream'

print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVER}...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_latest_valid_price():
    """
    Fetches the last 20 records and finds the newest one 
    that actually has a valid Price (not Null).
    """
    url = "https://api.energidataservice.dk/dataset/ImbalancePrice"
    params = {
        "limit": 20,  # Fetch deeper history to skip nulls
        "filter": '{"PriceArea":["DK1"]}',
        "sort": "TimeUTC DESC" 
    }
    
    try:
        response = requests.get(url, params=params)
        records = response.json().get('records', [])
        
        # Iterate through records to find the first non-null price
        for record in records:
            price = record.get('ImbalancePriceEUR')
            if price is not None:
                return record # Found it!
        
        print("⚠️ All 20 recent records had NULL prices.")
        return None

    except Exception as e:
        print(f"❌ Fetch Error: {e}")
        return None

# ---------------------------------------------------------
# THE LIVE LOOP
# ---------------------------------------------------------
print("📡 Starting Live Stream (Press Ctrl+C to stop)...")

while True:  # <--- CHANGED from range(5)
    data = get_latest_valid_price()
    
    if data:
        data['fetched_at'] = time.time()
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()
        print(f"🚀 Sent Market Price: {data['ImbalancePriceEUR']} EUR")
    else:
        print("⚠️ No valid data found.")
    
    time.sleep(60) # Wait 60 seconds (Real market speed)

print("\n✅ Phase 2 Complete: Valid stream established.")