from kafka import KafkaProducer  
import json  
import time  
import requests  
from datetime import datetime  

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {'vs_currency': 'usd', 'ids': 'bitcoin,ethereum,solana,tether'}  
headers = {"accept": "application/json", "x-cg-demo-api-key": "CG-KPEFjCbf7wwiYFUFHPF8fymY"}  

producer = KafkaProducer(
    bootstrap_servers='course-kafka:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

def fetch_and_publish():
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    for item in data:
        topic = f"crypto_{item['id']}"
        print(f"Producing message to {topic}: value={json.dumps(item)}")
        producer.send(topic, value=item)
    
    producer.flush()
    print("Published data successfully")

if __name__ == '__main__':
    fetch_and_publish()  # Call the function to fetch data and publish to Kafka
