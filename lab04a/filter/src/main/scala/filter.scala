package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filter extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()


  val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "lab04_input_data")
    .option("startingOffsets", "earliest")
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
    .json("lab04/buy/")

  dfParsed
    .filter(col("event_type") === "view")
    .write
    .mode("overwrite")
    .format("json")
    .partitionBy("p_date")
    .json("lab04/view/")

}