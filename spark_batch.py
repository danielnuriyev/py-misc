
###
# ~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1,org.apache.spark:spark-catalyst_2.11:2.2.1,com.datastax.spark:spark-cassandra-connector_2.11:2.0.7,org.slf4j:slf4j-api:1.7.25 spark_batch.py
###

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

if __name__ == '__main__':

    spark = SparkSession.builder\
        .appName("SimpleApp")\
        .getOrCreate()

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "devirlcmbcon001.cimba.cimpress.io:9092") \
        .option("subscribe", "test-daniel-py-ts") \
        .option("startingOffsets", "earliest")\
        .load()

    df = df.selectExpr("CAST(value AS STRING)")

    df.show()

    s = StructType([\
        StructField("t", FloatType()),\
        StructField("v", FloatType()),\
        StructField("k", StringType())])

    df = df.select(F.from_json(df.value, s).alias('json')).select('json.k', 'json.v', 'json.t')

    df.write.format('json') \
        .option('path', 'json') \
        .mode('append') \
        .save()

    df.write.format('org.apache.spark.sql.cassandra')\
        .options(table="test_ts", keyspace="test")\
        .mode('append')\
        .save()
