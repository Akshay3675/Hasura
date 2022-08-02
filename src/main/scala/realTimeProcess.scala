import org.apache.spark.sql.SparkSession
from pyspark.sql import SparkSession
import org.apache.spark.SparkConf

object realTimeProcess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local")

    val spark = SparkSession.builder.appName("test").config("spark.sql.debug.maxToStringFields", "100")
      .config(conf)
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
      .getOrCreate()

    val dsraw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.42.71.50:29092")
      .option("subscribe", "dbserver1.inventory.orders")
      .option("startingOffsets", "earliest")
      .load()


   val ds = dsraw.selectExpr("CAST(value AS STRING)")


  }
}





# kafka
dsraw = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "172.42.71.50:29092") \
.option("subscribe", "dbserver1.inventory.orders") \
.option("startingOffsets", "earliest") \
.load()


ds = dsraw.selectExpr("CAST(value AS STRING)")
