import org.apache.spark.sql.SparkSession

object batch {
  def main(agrs: Array[String]) = {

// configuraçoes do spark session para o node
    val spark = SparkSession
      .builder()
      .appName("Spark batch With Scala and Kafka")
      .master("spark://spark-master:7077")
      .getOrCreate()

    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")

// pegado dados de um node
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart")
      .option("includeHeaders", "true")
      .load()

    val rawDF = df.selectExpr("CAST(scala-message AS STRING)", "CAST(message AS STRING)").as[(String, String)]

// Escrevendo dados em um modelo 
    val query = rawDF.write
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
    
  }
}
