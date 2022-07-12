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
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

session = configure_spark_with_delta_pip(builder).getOrCreate()


raw_df = session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092") \
    .option("subscribe", "person") \
    .load()


df_l2 = raw_df.selectExpr("CAST(value AS STRING)")
split_col = split(df_l2.value, '\\|',)


#---------------------------------------------------------------------------------------------------------------------------

# if you want to append (include window and it's complexities)
df_l3 = df_l2.withColumn("EVENT_NAME", split_col.getItem(0)) \
    .withColumn("ATTEMPTS", split_col.getItem(1).cast("Integer")) \
    .withColumn("DURATION", split_col.getItem(2).cast("Integer")) \
    .withColumn("EVENT_TIME", split_col.getItem(3).cast("timestamp")) \
    .drop("value") \
    .withWatermark("EVENT_TIME", "60 minutes") \
    .groupBy("EVENT_NAME",window("EVENT_TIME", "30 minutes","5 minutes")).agg(sum("DURATION").alias("TOTAL_DURATION"))

df_l3.writeStream \
    .format("delta") \
    .option("checkpointLocation", "hdfs://master:9000/Delta/Checkpoint/person") \
    .outputMode("append") \
    .start("hdfs://master:9000/Delta/Data/person2") \
    .awaitTermination()

#---------------------------------------------------------------------------------------------------------------------------
# if you want to Upsert

# Creating the table first
DeltaTable.createOrReplace(session) \
    .addColumn("EVENT_NAME", "STRING") \
    .addColumn("TOTAL_DURATION", IntegerType()) \
    .property("description", "TEST-KAFKA-SPARK-DELTA") \
    .location("hdfs://192.168.49.13:9000/Delta/Data/person3") \
    .execute()


df_l3 = df_l2.withColumn("EVENT_NAME", split_col.getItem(0)) \
    .withColumn("ATTEMPTS", split_col.getItem(1).cast("Integer")) \
    .withColumn("DURATION", split_col.getItem(2).cast("Integer")) \
    .withColumn("EVENT_TIME", split_col.getItem(3).cast("timestamp")) \
    .drop("value") \
    .groupBy("EVENT_NAME").agg(sum("DURATION").alias("TOTAL_DURATION"))

result = DeltaTable.forPath(session, "hdfs://master:9000/Delta/Data/person3")
def upsertToDelta(df, batchId):
    result.alias("t").merge(
        df.alias("s"),
        "s.EVENT_NAME = t.EVENT_NAME") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

df_l3.writeStream \
    .format("delta") \
    .option("checkpointLocation", "hdfs://master:9000/Delta/Checkpoint/person3") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start() \
    .awaitTermination()