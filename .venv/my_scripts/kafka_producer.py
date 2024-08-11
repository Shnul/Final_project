from confluent_kafka import Producer
import requests
import json
import time

# CoinGecko API endpoint
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {'vs_currency': 'usd', 'ids': 'bitcoin,ethereum,solana,tether'}
headers = {"accept": "application/json", "x-cg-demo-api-key": "CG-KPEFjCbf7wwiYFUFHPF8fymY"}

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'my-producer',
    'acks': 'all',
    'retries': 5,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'gzip'
}

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Create Producer instance
producer = Producer(producer_config)

def fetch_and_publish():
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    for item in data:
        if item['id'] == 'bitcoin':
            topic = 'crypto_bitcoin'
        elif item['id'] == 'ethereum':
            topic = 'crypto_ethereum'
        elif item['id'] == 'solana':
            topic = 'crypto_solana'
        elif item['id'] == 'tether':
            topic = 'crypto_tether'
        else:
            continue
        
        key = f"{item['id']}_{int(time.time())}"
        print(f"Producing message to {topic}: key={key}, value={json.dumps(item)}")
        producer.produce(topic, key=key, value=json.dumps(item), callback=delivery_report)
    producer.flush()  # Ensure all messages are sent
    print(f"Published data: {data}")

if __name__ == '__main__':
    fetch_and_publish()