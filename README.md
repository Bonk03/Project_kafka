# Project_kafka

# How to set up project

In folder kafka-spark open terminal, then write this commands in exact order:

```docker compose up -d kafka; sets up kafka```

wait some time untill next command to be sure that kafka is running

```docker compose up -d producer```

make sure that producer is producing logs with next log

```docker compose logs -f producer```

run spark

```docker compose up spark```

after all these command you should see batches from last 10 minutes with sorted websites

# How does it work

producer.py

This script simulates web server access logs and sends them to an Apache Kafka topic in real time. It ensures that the target topic exists, connects to Kafka with retries (to handle startup delays), and continuously produces JSON-formatted log messages every second.

1. Topic Creation (create_topic_if_not_exists)

Uses KafkaAdminClient to check if the topic already exists.

If the topic is missing, it creates a new one.

This ensures the producer can always send messages to the expected topic.

2. Log Message Simulation

A predefined list of URLs (/home, /about, /contact, /products, /blog) and users (user1 through user10) are used to generate synthetic access logs.

Each log entry contains: timestamp (current UTC time in ISO format), url (randomly chosen page), user_id (randomly chosen user)

3. Message Sending Loop

Once started, the producer sends one log message per second to the Kafka topic.

After each send, it prints a confirmation to the console to be sure that topic was sent.

4. Error Handling & Recovery

If message sending fails (e.g., Kafka connection loss), the producer is reset to None.

The script retries creating a new KafkaProducer until Kafka is available again, then resumes sending logs.
