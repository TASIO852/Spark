from posixpath import abspath
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
import os
import findspark


os.environ["JAVA_HOME"] = "C:/Program Files/Java/jre1.8.0_351"
os.environ["SPARK_HOME"] = "C:/Users/tasio.guimaraes/Documents/Spark/spark-3.3.1-bin-hadoop3"

findspark.init()

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("Spark arquivo py") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12-3.3.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('INFO ')

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twet") \
        .option("startingOffsets", "earliest") \
        .load()
        
    # vendo os dados 
    df.createOrReplaceTempView("TwetView").show() 
    
    # Escrevendo em um topico kafka
    df.selectExpr('id', 'nome','assunto') \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "twet") \
    .save() 

# Tranformando em um DW a aplicaçao por um view

spark.stop()
