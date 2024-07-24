from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_TOPIC = 'twitter'
KAFKA_SERVER = 'localhost:9092'

# Set up Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='twitter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tweet = message.value
    print(f"Received tweet: {tweet['text']}")
