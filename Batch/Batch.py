import os
from pyspark.sql import SparkSession
import findspark

os.environ["JAVA_HOME"] = "C:/Program Files/Java/jre1.8.0_351"
os.environ["SPARK_HOME"] = "C:/Users/tasio.guimaraes/Documents/Spark/spark-3.3.1-bin-hadoop3"

findspark.init()

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Spark arquivo py") \
    .getOrCreate()

spark