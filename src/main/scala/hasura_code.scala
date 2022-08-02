import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object hasura_code {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val path = args(0)
    //val df = spark.read.option("delimiter", "|").csv("s3://pfg-datadump/unsaved/test/")
    val df = spark.read.option("delimiter", "|").csv(path)
    val getJsonContent = udf { input: String => input.substring(input.indexOf("{")) }
    val df1 = df.withColumn("json_content", getJsonContent(col("_c0"))).drop("_c0")

    val schema = new StructType().add("project_id", StringType, true).add("timestamp", StringType, true).add("operation", new StructType()
      .add("name" , StringType)
      .add("runtime" , DoubleType)
      .add("request_id" , StringType)
      .add("response_size" , IntegerType)
      .add("request_size" , IntegerType)
      .add("http_status" , IntegerType)
    )

    val df2 = df1.withColumn("json_columns", from_json($"json_content", schema))
    val df3=df2.select(col("json_columns.*"))
    val df4=df3.select($"project_id", $"timestamp", $"operation.*")
    val df5 = df4.withColumn("x_date",
      concat_ws(" "
        , split(split($"timestamp", "\\+").getItem(0), "T").getItem(0)
        , split(split($"timestamp", "\\+").getItem(0), "T").getItem(1))
        .cast(TimestampType)).withColumn("timestamp",
      from_unixtime(
        unix_timestamp($"x_date", "yyyy-MM-dd HH:mm:ss.SSSS")
        , "yyyy-MM-dd HH:mm:ss")).drop("x_date").withColumn("request_date", to_date($"timestamp"))

    df5.groupBy("project_id","request_date").count().show

  }
}