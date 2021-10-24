import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._

object filter extends App {

  val conf = new SparkConf().setAppName("spark-test").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val offsetStr = conf.getOption("spark.filter.offset").getOrElse("earliest")
  val offset = { if (offsetStr == "earliest") offsetStr
  else """{"lab04_input_data":{"0":""" + offsetStr + """}}"""}
  val prefix = conf.getOption("spark.filter.output_dir_prefix").getOrElse("visits")


  val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "lab04_input_data")
    .option("startingOffsets", offset)
    .option("endingOffsets", "latest")
    .load()

  val schema = StructType(Seq(
    StructField("event_type", StringType, true),
    StructField("category", StringType, true),
    StructField("item_id", StringType, true),
    StructField("item_price", IntegerType, true),
    StructField("uid", StringType, true),
    StructField("timestamp", LongType, true)))

  val dfParsed = df.select(col("value").cast(StringType))
    .withColumn("jsonData", from_json(col("value"), schema))
    .select(col("jsonData.category"), col("jsonData.event_type"), col("jsonData.item_id"), col("jsonData.item_price").cast(StringType),
      col("jsonData.timestamp"), col("jsonData.uid"))
    .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))
    .withColumn("p_date", col("date"))

  dfParsed
    .filter(col("event_type") === "buy")
    .write
    .mode("overwrite")
    .format("json")
    .partitionBy("p_date")
    .json(prefix + "/buy/")

  dfParsed
    .filter(col("event_type") === "view")
    .write
    .mode("overwrite")
    .format("json")
    .partitionBy("p_date")
    .json(prefix + "/view/")

}