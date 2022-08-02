import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object batchProcess {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    /*
    val path = args(0)
    val end_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.parse(args(1).replace(" ","T")))
    val end_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.parse(args(2).replace(" ","T")))
    */

    // STEP 1 :read file
    val path = "s3://pfg-datadump/unsaved/test/"
    val start_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.parse("2022-07-30 00:00:00".replace(" ","T")))
    val end_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.parse("2022-08-01 00:00:00".replace(" ","T")))

    // STEP 2 : parse json create df
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

    // STEP 3 : clean timestamp column and add bill_date column

    val df5 = df4.withColumn("x_date",
      concat_ws(" "
        , split(split($"timestamp", "\\+").getItem(0), "T").getItem(0)
        , split(split($"timestamp", "\\+").getItem(0), "T").getItem(1))
        .cast(TimestampType)).withColumn("timestamp",
      from_unixtime(
        unix_timestamp($"x_date", "yyyy-MM-dd HH:mm:ss.SSSS")
        , "yyyy-MM-dd HH:mm:ss")).withColumn("bill_date",
      to_date($"timestamp")).withColumn("timestamp", to_timestamp($"timestamp"))

    //Step 4: Group by required columns to calculate bill
    val result = df5.filter($"timestamp" >= start_time
      && $"timestamp" < end_time).groupBy("project_id",
      "bill_date").agg(
      sum($"response_size").as("total_response_size"),
      sum($"request_size").as("total_request_size"),
      count("project_id").as("project_io"))

    result.orderBy("bill_date").withColumn("bill_start_time",
      lit(start_time)).withColumn("bill_end_time",
      lit(end_time)).show(false)

    val result_status_wise = df5.filter($"timestamp" >= start_time
      && $"timestamp" < end_time).groupBy("project_id",
      "bill_date", "http_status").agg(
      sum($"response_size").as("total_response_size"),
      sum($"request_size").as("total_request_size"),
      count("project_id").as("project_io"))

    result_status_wise.withColumn("bill_start_time",
      lit(start_time)).withColumn("bill_end_time",
      lit(end_time)).orderBy("project_id").show(false)
  }
}