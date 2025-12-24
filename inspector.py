import requests
import json

print("🕵️ Inspecting API Data...")

# Fetch 1 record from ImbalancePrice
url = "https://api.energidataservice.dk/dataset/ImbalancePrice"
params = {
    "limit": 1,
    "filter": '{"PriceArea":["DK1"]}'
}

try:
    response = requests.get(url, params=params)
    data = response.json().get('records', [])[0]
    
    print("\n--- RAW DATA FROM API ---")
    print(json.dumps(data, indent=4))
    print("-------------------------\n")
    
except Exception as e:
    print(f"❌ Error: {e}")