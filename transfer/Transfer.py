from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf
_conf = SparkConf()
# _conf.setAppName("DEDUPLICATE")

builder = SparkSession. \
    builder. \
    config(conf=_conf)
session = builder.getOrCreate()

# .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1')

print("Start of Micro Batches")

df_final = session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("subscribe", "input") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss","true")\
    .option("maxOffsetsPerTrigger",4500000)\
    .load()\
    .selectExpr("CAST(value AS STRING)")




#df_l2=df.select(from_json(col("value"), fileschema).alias("data")).select("data.*")




df_final.writeStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")\
    .option("checkpointLocation", "hdfs://172.17.135.31:9000/Checkpoints/jusfdtTransfersdf")\
    .outputMode("append")\
    .option("topic", "fake")\
    .start()\
    .awaitTermination()
    
