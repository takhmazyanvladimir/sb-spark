import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import sys.process._

object agg extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  def createConsoleSink(chkName: String, mode: String, df: DataFrame) = {
    df
      .writeStream
      .outputMode(mode)
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"/user/vladimir.takhmazyan/tmp/chk/$chkName")
      .option("truncate", "false")
  }

  def createKafkaSink(chkName: String, mode: String, df: DataFrame) = {
    df
      .writeStream
      .outputMode(mode)
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "vladimir_takhmazyan_lab04b_out")
      .option("checkpointLocation", s"/user/vladimir.takhmazyan/tmp/chk/$chkName")
      .option("truncate", "false")
  }

  def killAll() = {
    SparkSession
      .active
      .streams
      .active
      .foreach { x =>
        val desc = x.lastProgress.sources.head.description
        x.stop
        println(s"Stopped ${desc}")
      }
  }

  val sdf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "vladimir_takhmazyan")
    .load()

  val schema = new StructType()
    .add("event_type", StringType, true)
    .add("category", StringType, true)
    .add("item_id", StringType, true)
    .add("item_price", IntegerType, true)
    .add("uid", StringType, true)
    .add("timestamp", LongType, true)

  val dfParsed = sdf.select(col("value").cast(StringType))
    .withColumn("jsonData",from_json(col("value"),schema))
    .select(col("jsonData.category"),col("jsonData.event_type"),col("jsonData.item_id"),col("jsonData.item_price").cast(StringType),
      from_unixtime(col("jsonData.timestamp")/1000).as("timestamp"),col("jsonData.uid"))

  val dfAgg = dfParsed
    .groupBy(window(col("timestamp"), "1 hours", "1 hours"))
    .agg(
      sum(when(col("event_type") === lit("buy"),(col("item_price")))).as("revenue"),
      sum(when(col("uid").isNotNull,1)).as("visitors"),
      sum(when(col("event_type") === lit("buy"),1)).as("purchases")
    )
    .withColumn("aov",col("revenue")/col("purchases"))
    .withColumn("start_ts",unix_timestamp(col("window.start")))
    .withColumn("end_ts",unix_timestamp(col("window.end")))
    .select("start_ts","end_ts","revenue","visitors","purchases","aov")

  val dfAggJson = dfAgg.select(to_json(struct(dfAgg.columns.map(column):_*)).alias("value"))

  "hdfs dfs -rm -r /user/vladimir.takhmazyan/tmp/chk".!!

  createKafkaSink("state2", "update", dfAggJson).start

  createConsoleSink("state2", "update", dfAggJson).start

}