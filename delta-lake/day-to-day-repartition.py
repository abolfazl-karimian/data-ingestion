from pyspark.sql.functions import *
from pyspark.conf import SparkConf
_conf = SparkConf()
_conf.setAppName("SP_Delta")
from pyspark.sql import SparkSession
from delta import *
builder = SparkSession. \
    builder. \
    config(conf=_conf) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.read \
    .format("delta") \
    .load("hdfs://master:9000/delta/data/ref_data") \
    .orderBy(col("type_id")) \
    .repartition(10) \
    .write \
    .option("dataChange", "false") \
    .format("delta") \
    .mode("overwrite") \
    .save("hdfs://master:9000/delta/data/ref_data")