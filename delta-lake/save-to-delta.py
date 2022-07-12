from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

_conf = SparkConf()
_conf.setAppName("SaveToDelta")
_conf.set("spark.sql.streaming.stateStore.providerClass",
          "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

builder = SparkSession. \
    builder. \
    config(conf=_conf) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql- kafka-0-10_2.12:3.1.0') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

session = configure_spark_with_delta_pip(builder).getOrCreate()

input_df = session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
    .option("subscribe", "churn_result") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetPerTrigger", 4000000) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

schema = (StructType()
          .add(StructField("MSISDN", StringType()))
          .add(StructField("CALL_PARTNER", StringType()))
          .add(StructField("DURATION", IntegerType()))
          .add(StructField("CDR_TYPE", StringType()))
          .add(StructField("IMSI", StringType()))
          .add(StructField("SEQ_NO", IntegerType()))
          .add(StructField("RECORD_DATE", TimestampType()))
          .add(StructField("LAST_TIME_RECEIVED", TimestampType()))
          )

df = input_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "hdfs://master:9000/delta/checkpoint/churn_result") \
    .outputMode("append") \
    .start("hdfs://master:9000/delta/data/churn_result") \
    .awaitTermination()
