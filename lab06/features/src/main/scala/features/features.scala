import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object features extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()


  val weblogs = spark.read.json("/labs/laba03/weblogs.json")
    .select(col("uid"),explode(col("visits")).as("visit"))
    .select(col("uid"),col("visit.timestamp"),col("visit.url"))
    .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
    .withColumn("domain", regexp_replace(col("host"), "www.", ""))
    .withColumn("dayOfWeek",dayofweek(from_unixtime(col("timestamp")/1000)))
    .withColumn("hour",hour(from_unixtime(col("timestamp")/1000)))
    .withColumn("work_hour",when(col("hour") >= 9 && col("hour") < 18, 1).otherwise(0))
    .withColumn("evening_hour",when(col("hour") >= 18, 1).otherwise(0))

  val hourMetrics = weblogs
    .filter(col("uid").isNotNull)
    .withColumn("hourFeat",concat(lit("web_hour_"),col("hour").cast(StringType)))
    .groupBy(col("uid")).pivot("hourFeat").count()

  print(hourMetrics.count())
  hourMetrics.persist()

  val otherMetrics = weblogs
    .filter(col("uid").isNotNull)
    .groupBy(col("uid"))
    .agg(
      sum(when(col("dayOfWeek") === 2, 1).otherwise(0)).as("web_day_mon"),
      sum(when(col("dayOfWeek") === 3, 1).otherwise(0)).as("web_day_tue"),
      sum(when(col("dayOfWeek") === 4, 1).otherwise(0)).as("web_day_wed"),
      sum(when(col("dayOfWeek") === 5, 1).otherwise(0)).as("web_day_thu"),
      sum(when(col("dayOfWeek") === 6, 1).otherwise(0)).as("web_day_fri"),
      sum(when(col("dayOfWeek") === 7, 1).otherwise(0)).as("web_day_sat"),
      sum(when(col("dayOfWeek") === 1, 1).otherwise(0)).as("web_day_sun"),
      (sum(col("work_hour"))/count("*")).as("web_fraction_work_hours"),
      (sum(col("evening_hour"))/count("*")).as("web_fraction_evening_hours")
    )

  print(otherMetrics.count())
  otherMetrics.persist()

  val popularDomains = weblogs
    .filter(col("domain").isNotNull)
    .groupBy(col("domain"))
    .agg(count("*").as("cn"))
    .orderBy(desc("cn"))
    .limit(1000)
    .select(col("domain"))
    .orderBy(asc("domain"))

  print(popularDomains.count())
  popularDomains.persist()

  val usersDomains = weblogs
    .filter(col("uid").isNotNull)
    .join(popularDomains,Seq("domain"),"inner")
    .groupBy(col("uid"),col("domain"))
    .agg(count("*").as("cn"))

  val users = weblogs
    .filter(col("uid").isNotNull)
    .select(col("uid").as("uid"))
    .distinct

  print(users.count())
  users.persist()

  val usersDomainsAgg = users
    .crossJoin(popularDomains)
    .join(usersDomains,Seq("uid","domain"),"left")
    .na.fill(0)
    .orderBy(asc("uid"),asc("domain"))
    .groupBy(col("uid"))
    .agg(collect_list(col("cn")).as("domain_features"))

  print(usersDomainsAgg.count())
  usersDomainsAgg.persist()

  val allNewMetrics = usersDomainsAgg
    .join(hourMetrics,Seq("uid"),"inner")
    .join(otherMetrics,Seq("uid"),"inner")

  val usersItems = spark.read.parquet("hdfs:///user/vladimir.takhmazyan/users-items/20200429/")

  usersItems
    .join(allNewMetrics,Seq("uid"),"full")
    .write.parquet("hdfs:///user/vladimir.takhmazyan/features/")


}