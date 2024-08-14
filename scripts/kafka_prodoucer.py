from kafka import KafkaProducer  # Import KafkaProducer for sending messages to Kafka topics
import json  # Import json for encoding the data
import time  # Import time for generating timestamps
import requests  # Import requests to make HTTP requests
from datetime import datetime  # Import datetime for timestamp-related operations (currently not used)

# CoinGecko API endpoint
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {'vs_currency': 'usd', 'ids': 'bitcoin,ethereum,solana,tether'}  # Parameters to specify the currencies and coins
headers = {"accept": "application/json", "x-cg-demo-api-key": "CG-KPEFjCbf7wwiYFUFHPF8fymY"}  # Headers for the API request

# Create KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers='course-kafka:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the data to JSON and encode it as UTF-8 bytes
)

def fetch_and_publish():
    # Send a GET request to the CoinGecko API to fetch cryptocurrency data
    response = requests.get(url, headers=headers, params=params)
    
    # Parse the JSON response
    data = response.json()
    
    # Iterate over each cryptocurrency item in the response
    for item in data:
        # Construct the Kafka topic name dynamically based on the cryptocurrency ID
        topic = f"crypto_{item['id']}"
        
        # Log the data being sent to the Kafka topic
        print(f"Producing message to {topic}: value={json.dumps(item)}")
        
        # Send the message to the appropriate Kafka topic
        producer.send(topic, value=item)
    
    # Ensure all messages are sent before exiting
    producer.flush()
    
    # Log successful publication
    print("Published data successfully")

# Main entry point
if __name__ == '__main__':
    fetch_and_publish()  # Call the function to fetch data and publish to Kafka
