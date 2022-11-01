docker run -it spark \
spark-submit \
--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0" \
--master "spark://spark-master:7077" \
--py-files ConsumerBatch.py

##############################!
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 ConsumerBatch.py
##############################!


