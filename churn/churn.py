from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf

_conf = SparkConf()
_conf.setAppName("CHURN_RESULT")
_conf.set("spark.sql.streaming.stateStore.providerClass",
          "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

builder = SparkSession. \
    builder. \
    config(conf=_conf) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql- kafka-0-10_2.12:3.1.0')
session = builder.getOrCreate()
session.sparkContext.setLogLevel("WARN")

df = session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("subscribe", "fake") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10000000) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

fileSchema = (StructType()
              .add(StructField("MSISDN", StringType()))
              .add(StructField("CALL_PARTNER", StringType()))
              .add(StructField("DURATION", IntegerType()))
              .add(StructField("CDR_TYPE", StringType()))
              .add(StructField("IMSI", StringType()))
              .add(StructField("SEQ_NO", IntegerType()))
              .add(StructField("RECORD_DATE", TimestampType()))
              )

df = df.select(from_json(col("value"), fileSchema).alias("data")).select("data.*")

df = df \
    .withColumn("duration1", when(col("CDR_TYPE") == "1", col("DURATION")).otherwise(lit(0))) \
    .withColumn("duration2", when(col("CDR_TYPE") == "2", col("DURATION")).otherwise(lit(0))) \
    .withColumn("duration3", when(col("CDR_TYPE") == "3", col("DURATION")).otherwise(lit(0))) \
    .withColumn("duration4", when(col("CDR_TYPE") == "4", col("DURATION")).otherwise(lit(0))) \
    .withColumn("duration5", when(col("CDR_TYPE") == "5", col("DURATION")).otherwise(lit(0))) \
    .withColumn("count1", when(col("CDR_TYPE") == "1", lit(1)).otherwise(lit(0))) \
    .withColumn("count2", when(col("CDR_TYPE") == "2", lit(1)).otherwise(lit(0))) \
    .withColumn("count3", when(col("CDR_TYPE") == "3", lit(1)).otherwise(lit(0))) \
    .withColumn("count4", when(col("CDR_TYPE") == "4", lit(1)).otherwise(lit(0))) \
    .withColumn("count5", when(col("CDR_TYPE") == "5", lit(1)).otherwise(lit(0))) \
    .withColumn("_DATE", date_format(col("RECORD_DATE"), "yyyy-MM-dd 00:00:00"))

df = df.withWatermark("RECORD_DATE", "24 hours") \
    .dropDuplicates(["MSISDN", "CALL_PARTNER", "DURATION", "CDR_TYPE", "IMSI", "SEQ_NO", "RECORD_DATE"]) \
    .groupBy("_DATE", "MSISDN", "IMSI") \
    .agg(
    sum("duration1").alias("TOTAL_DURATION1"),
    sum("duration2").alias("TOTAL_DURATION2"),
    sum("duration3").alias("TOTAL_DURATION3"),
    sum("duration4").alias("TOTAL_DURATION4"),
    sum("duration5").alias("TOTAL_DURATION5"),
    sum("count1").alias("TOTAL_COUNT1"),
    sum("count2").alias("TOTAL_COUNT2"),
    sum("count3").alias("TOTAL_COUNT3"),
    sum("count4").alias("TOTAL_COUNT4"),
    sum("count5").alias("TOTAL_COUNT5"),
    max("RECORD_DATE").alias("LAST_TIME_RECEIVED"))

df = df.select(to_json(struct("MSISDN", "IMSI", "_DATE",
                              "TOTAL_DURATION1", "TOTAL_DURATION2", "TOTAL_DURATION3", "TOTAL_DURATION4",
                              "TOTAL_DURATION5",
                              "TOTAL_COUNT1", "TOTAL_COUNT2", "TOTAL_COUNT3", "TOTAL_COUNT4", "TOTAL_COUNT5",
                              "LAST_TIME_RECEIVED")) \
               .alias("value")) \
    .selectExpr("CAST(value AS STRING)")

df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("checkpointLocation", "hdfs://172.17.135.31:9000/Checkpoints/churn_result") \
    .outputMode("update") \
    .option("topic", "churn_result") \
    .start() \
    .awaitTermination()
