from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import max as spark_max

spark = SparkSession.builder.appName("KafkaWebLogAggregator").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("url", StringType()),
    StructField("user_id", StringType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web_logs") \
    .option("startingOffsets", "earliest") \
    .load()

logs = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.timestamp").cast(TimestampType()).alias("timestamp"),
        col("data.url").alias("url")
    )

windowDuration = "10 minutes"
slideDuration = "5 seconds"

agg = logs.groupBy(
    window(col("timestamp"), windowDuration, slideDuration),
    col("url")
).count()

filtered_agg = agg.filter(
    (col("window.end") >= expr("current_timestamp()")) &
    (col("window.end") <= expr(f"current_timestamp() + interval {slideDuration}"))
)

top_urls = filtered_agg.orderBy(col("count").desc()).limit(3)


query = top_urls.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
