from confluent_kafka import Consumer, KafkaError, KafkaException
import signal
import sys

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'api.version.request': False  # Add this line
}

# Create Consumer instance
consumer = Consumer(consumer_config)

# Subscribe to topics
consumer.subscribe(['crypto_bitcoin', 'crypto_ethereum', 'crypto_solana', 'crypto_tether'])

def process_message(msg):
    # Process the message (e.g., print it, store it in a database, etc.)
    print(f"Received message: {msg.value().decode('utf-8')}")

def consume_messages():
    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout in seconds
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                process_message(msg)
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def signal_handler(sig, frame):
    print('You pressed Ctrl+C! Exiting gracefully...')
    consumer.close()
    sys.exit(0)

if __name__ == '__main__':
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consume_messages()