import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._

object users_items extends App {

  val conf = new SparkConf().setAppName("users_items").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val input_dir = conf.getOption("spark.users_items.input_dir").getOrElse("hdfs:///user/vladimir.takhmazyan/visits")
  val output_dir = conf.getOption("spark.users_items.output_dir").getOrElse("hdfs:///user/vladimir.takhmazyan/users-items")
  val update = conf.getOption("spark.users_items.update").getOrElse(1)

  if (update == 0) {
    val dfBuy = spark.read.json("hdfs:///user/vladimir.takhmazyan/visits/buy/")

    val dfView = spark.read.json("hdfs:///user/vladimir.takhmazyan/visits/view/")

    val pivotBuy = dfBuy
      .filter(col("uid").isNotNull)
      .select(col("uid"), regexp_replace(lower(concat(lit("buy_"), col("item_id"))), "[ -]", "_").as("item_id"), lit(1).as("amount"))
      .groupBy("uid").pivot("item_id").sum("amount")

    val pivotView = dfView
      .filter(col("uid").isNotNull)
      .select(col("uid"), regexp_replace(lower(concat(lit("view_"), col("item_id"))), "[ -]", "_").as("item_id"), lit(1).as("amount"))
      .groupBy("uid").pivot("item_id").sum("amount")

    val pivotAll = pivotBuy.
      join(pivotView, Seq("uid"), "full")
      .na.fill(0)

    pivotAll
      .write
      .mode("overwrite")
      .parquet("hdfs:///user/vladimir.takhmazyan/users-items/20200429/")

  }

  else {

    val pivotOld = spark.read.parquet(output_dir + "/20200429/")

    val dfBuyNew = spark.read.json(input_dir + "/buy/")

    val dfViewNew = spark.read.json(input_dir + "/view/")

    val newItemsPrep = dfBuyNew.union(dfViewNew)
      .select(col("uid"), regexp_replace(lower(concat(col("event_type"), lit("_"), col("item_id"))), "[ -]", "_").as("item_id"), lit(1).as("amount"))

    var oldItemsPrep = newItemsPrep.limit(0)

    for (c <- pivotOld.columns) {

      if (c != "uid") {
        var df = pivotOld.select(col("uid"), lit(c).as("item_id"), col(c).as("amount"))
          .filter(col("amount") > 0)
        oldItemsPrep = df.union(oldItemsPrep)
      }
    }

    val newPivot = oldItemsPrep.
      union(newItemsPrep)
      .groupBy("uid").pivot("item_id").sum("amount")

    newPivot
      .write
      .mode("overwrite")
      .parquet(output_dir + "/20200430/")

  }
}