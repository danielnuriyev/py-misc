
###
# ~/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 spark_stream.py
###

from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SimpleApp").config(key="--jars", value="spark-sql-kafka-0-10_2.11-2.3.0.jar") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "devirlcmbcon001.cimba.cimpress.io:9092") \
        .option("subscribe", "cimba_raw_inbox") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    q = df.writeStream.format('json')\
        .option("path","test-spark-out")\
        .option("checkpointLocation", "./spark-checkpoints")\
        .trigger(processingTime='5 seconds').start()

    q.awaitTermination()
