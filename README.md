# Project_kafka

## Authors

Bartłomiej Bąk 193634

Mikołaj Goździelewski 193263

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

## producer.py

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



## spark_job.py

This script consumes web access logs from a Kafka topic, aggregates them in real time with Spark Structured Streaming, and continuously outputs the top three most-visited URLs in recent time windows to the console.

1. Kafka Stream Ingestion

Connects to a Kafka broker at kafka:9092.

Subscribes to the topic web_logs.

Reads all available messages from the earliest offset.

Each Kafka message contains a JSON payload with a timestamp, URL, and user ID.

2. JSON Parsing and Data Preparation

Converts the Kafka message value (binary) into a string.

Parses the JSON using a predefined schema (timestamp, url, user_id).

Casts the timestamp into Spark’s native TimestampType.

Keeps only timestamp and url fields for aggregation.

3. Sliding Window Aggregation

Groups log records into 10-minute windows that slide every 5 seconds.

Counts the number of times each URL is accessed within each window.

Ensures results are continuously updated as new events arrive.

4. Filtering Current Results

Filters out old windows, keeping only those that are ending near the current time.

This ensures the aggregation focuses on the most recent user activity.

5. Top URL Ranking

Sorts the filtered results by descending access count.

Selects the top 3 most-visited URLs for the current window.

6. Console Output

Writes results to the console in complete output mode, refreshing every 5 seconds.

Displays the window time range, URL, and count of visits.

Runs continuously until manually stopped.
