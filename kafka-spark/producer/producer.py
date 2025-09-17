from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json, random, time, sys
from datetime import datetime

def create_topic_if_not_exists(topic_name, bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        admin_client.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        print(f"Created missing topic: {topic_name}", flush=True)
    else:
        print(f"Topic already exists: {topic_name}", flush=True)
    admin_client.close()

topic = "web_logs"
bootstrap_servers = "kafka:9092"
create_topic_if_not_exists(topic, bootstrap_servers)

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception:
        print("Kafka not ready, retrying in 2s...", flush=True)
        time.sleep(2)

urls = ['/home', '/about', '/contact', '/products', '/blog']
users = [f"user{i}" for i in range(1, 11)]

print("Producer started, sending logs every second...", flush=True)

while True:
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "url": random.choice(urls),
        "user_id": random.choice(users)
    }
    try:
        producer.send(topic, log)
        print(f"Sent: {log}", flush=True)
    except Exception as e:
        print("Error sending message, retrying...", e, flush=True)
        producer = None
        while producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
            except Exception:
                time.sleep(2)
    time.sleep(1)
