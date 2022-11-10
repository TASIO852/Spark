from pyspark.sql import SparkSession
from pyspark.sql.streaming import SparkStreaming
import findspark
import os

os.environ["JAVA_HOME"] = "C:/Program Files/Java/jre1.8.0_351"
os.environ["SPARK_HOME"] = "C:/Users/tasio.guimaraes/Documents/Spark/spark-3.3.1-bin-hadoop3"

findspark.init()

spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "spark-sql-kafka-0-10_2.12-3.3.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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

raw = spark.sql("select * from `kafka-streaming-messages`")
raw.show()

query.awaitTermination()
