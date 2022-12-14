import logging

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

# def func_call(df):
#     df.selectExpr("CAST(value AS STRING) as json")
#     requests = df.rdd.map(lambda x: x.value).collect()
#     logging.info(requests)

spark = SparkSession \
    .builder \
    .appName("hasura") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf()) \
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .getOrCreate()

#STEP1 subscribe to kafka topic
subscribe_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "hasura.training.dataset") \
    .option("startingOffsets", "earliest") \
    .load()

#STEP2 create dataframe from log data

ds = subscribe_df.selectExpr("CAST(value AS STRING)")

#STEP3 clean data and apply transformation

apply_transformation_df = ds \
    .writeStream \
    .queryName("apply_transformation_query") \
    .format("memory") \
    .start()

result_df = spark.sql("select * from apply_transformation_query")

result_df.show()

#STEP4 write df to target location
result_df.write \
    .format('parquet') \
    .mode('append') \
    .save('s3://datadump/unsaved/streaming/')

