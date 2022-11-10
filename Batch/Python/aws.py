
import argparse
import logging
from operator import add
from random import random

from pyspark.sql import SparkSession

import findspark
import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jre1.8.0_351"
os.environ["SPARK_HOME"] = "C:/Users/tasio.guimaraes/Documents/Spark/spark-3.3.1-bin-hadoop3"

findspark.init()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def calculate_pi(partitions, output_uri):
    """
    Calculates pi by testing a large number of random numbers against a unit circle
    inscribed inside a square. The trials are partitioned so they can be run in
    parallel on cluster instances.

    :param partitions: The number of partitions to use for the calculation.
    :param output_uri: The URI where the output is written, typically an Amazon S3
                       bucket, such as 's3://example-bucket/pi-calc'.
    """
    def calculate_hit(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

    tries = 100000 * partitions
    logger.info(
        "Calculating pi with a total of %s tries in %s partitions.", tries, partitions)
    with SparkSession.builder.appName("My PyPi").getOrCreate() as spark:
        hits = spark.sparkContext.parallelize(range(tries), partitions)\
            .map(calculate_hit)\
            .reduce(add)\
            .read\
            .format("s3selectCSV")\
            .schema(...) \
            .options(...) \
            .load("s3://path/to/my/datafiles")
        pi = 4.0 * hits / tries
        logger.info("%s tries and %s hits gives pi estimate of %s.",
                    tries, hits, pi)
        if output_uri is not None:
            df = spark.createDataFrame(
                [(tries, hits, pi)], ['tries', 'hits', 'pi'])
            df.write.mode('overwrite').json(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--partitions', default=2, type=int,
        help="The number of parallel partitions to use when calculating pi.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    calculate_pi(args.partitions, args.output_uri)
