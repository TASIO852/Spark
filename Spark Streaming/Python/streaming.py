from pyspark.sql import SparkSession
from pyspark.sql.streaming import *

spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "quickstart") \
    .option("startingOffsets", "earliest") \
    .load()

df.selectExpr("CAST(python-message AS STRING)", "CAST(message AS STRING)")

query = df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# raw = spark.sql("select * from `kafka-streaming-messages`")
# raw.show()

query.awaitTermination()
