from posixpath import abspath
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *

warehause_location = abspath('spark-warehause')

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("Batch") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.warehouse.dir", warehause_location) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('INFO ')

#! lembrar de especificar o schema do kafka quando for ler do dbmaker tanto no produtor quanto no consumer
    # json_schema = StructType(
    #     [
    #         StructField('userid', InterType(), True)

    #     ]

    # )

# lendo dados em 1 tópico com o consumer spark  lendo todo o topic
# o consumer tem que ler a mesma coisa que o produtor escreveu referente ao tipo de arquivo e dado

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "quickstart") \
        .option("startingOffsets", "earliest") \
        .load()
    # .select(from_json(col("value").cast("string"),json_schema,jsonOptions).alias("user")) dados lidos em json
    df.selectExpr("CAST(python-message AS STRING)",
                  "CAST(message AS STRING)")  # Lidos em spark sql

    #! Etapa de tansformation

    get_coluns = df.select(
        col(nomecolun).alias("troca de nome a coluna"),
    )

# Tranformando em um DW a aplicaçao por um view

    get_coluns.createOrReplaceTempView("dwColun")

    get_group_df = spark.sql(
        """
    SELECT
        VDTRAVEI_COD AS 'PLACA',
        VDTRAVEI_MARCA AS 'MARCA_VEICULO',
        VDTRAVEI_MODELO AS 'MODELO_VEICULO',
        VDTRAVEI_LOCAL AS 'LOCAL_EMPLACAMENTO',
        VDTRAVEI_ESTADO_PLACA 'ESTADO_EMPLACAMENTO',
        VDTRAVEI_CARGA AS 'CARGA_VEICULO',
        VDTRAVEI_COR AS 'COR_VEICULO',
        VDTRAVEI_MOT_PRINC AS 'MOTORISTA_PRINCIPAL',
        VDTRAVEI_AJ1_PRINC AS 'AJUDANTE_PRINCIPAL',
        VDTRAVEI_RENAVAM AS 'RENAVAM',
        VDTRAVEI_CAPKG AS 'CAPACIDADE_QUILOS'

    FROM DBCONTROL2016001.CADVEI01
    """
    )

# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# escrevendo em um banco de dados postgres
    query = df \
        .write \
        .format("jbdc") \
        .mode("overtime") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "public.view_per_country_batch") \
        .option("user", "timedados") \
        .option("password", "ugauga") \
        .option("batchsize", 1000) \
        .save()


    # spark.stop()

    # raw = spark.sql("select * from `kafka-streaming-messages`")
    # raw.show()

    # query.awaitTermination()
