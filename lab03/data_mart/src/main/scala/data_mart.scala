import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.URL

object data_mart extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val url2Host = udf((inputUrl: String) => {
    try {
      val url = new URL(inputUrl)
      url.getHost()
    }
    catch {
      case e: Throwable => "no host"
    }
  }
  )

  val clients = spark.read.format("org.apache.spark.sql.cassandra")
    .option("keyspace", "labdata")
    .option("table", "clients")
    .option("spark.cassandra.connection.host", "10.0.0.5")
    .option("spark.cassandra.connection.port", "9042")
    .load()

  val visits = spark.read.format("org.elasticsearch.spark.sql")
    .option("es.nodes", "10.0.0.5")
    .option("es.port", "9200")
    .option("es.net.http.auth.user", "vladimir.takhmazyan")
    .option("es.net.http.auth.pass", "R9CQBEcS")
    .load("visits")
    .filter(col("uid").isNotNull)
    .select(col("uid"), regexp_replace(lower(col("category")), "[ -]", "_").as("category"))

  val domain_cats = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
    .option("dbtable", "domain_cats")
    .option("driver", "org.postgresql.Driver")
    .option("user", "vladimir_takhmazyan")
    .option("password", "R9CQBEcS")
    .load()

  val weblogs = spark.read.json("/labs/laba03/weblogs.json")

  val webLogsWithCats = weblogs
    .select(col("uid"), explode(col("visits")).as("visit"))
    .select(col("uid"), col("visit.timestamp"), col("visit.url"), regexp_replace(url2Host(col("visit.url")), "^www.", "").as("domain"))
    .join(domain_cats, Seq("domain"), "inner")

  val dfDomainCatsList = domain_cats.
    select(col("category")).
    distinct.
    orderBy(col("category"))

  val queryTextWebCatPart = dfDomainCatsList
    .select(concat(lit(", sum(case when category = \""), col("category"), lit("\" then 1 else 0 end) as web_"), col("category")).as("c"))
    .agg(concat_ws("\r\n", collect_list(col("c"))).as("c"))
    .head(1)(0)(0)

  val queryWebText = "select uid" + queryTextWebCatPart + " from webLogsWithCats group by uid"

  webLogsWithCats.createOrReplaceTempView("webLogsWithCats")
  val usersWebCats = spark.sql(queryWebText)

  val dfShopCatsList = visits.
    select(col("category")).
    distinct.
    orderBy(col("category"))

  val queryTextShopCatPart = dfShopCatsList
    .select(concat(lit(", sum(case when category = \""), col("category"), lit("\" then 1 else 0 end) as shop_"), col("category")).as("c"))
    .agg(concat_ws("\r\n", collect_list(col("c"))).alias("c"))
    .head(1)(0)(0)

  val queryShopText = "select uid" + queryTextShopCatPart + " from visits group by uid"

  visits.createOrReplaceTempView("visits")
  val usersShopCats = spark.sql(queryShopText)

  val result = clients
    .withColumn("age_cat",
      when(col("age") <= 24, "18-24").
        when(col("age") <= 34, "25-34").
        when(col("age") <= 44, "35-44").
        when(col("age") <= 54, "45-54").
        otherwise(">=55")
    )
    .drop("age")
    .join(usersShopCats, Seq("uid"), "left")
    .join(usersWebCats, Seq("uid"), "left")
    .na.fill(0)

  result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/vladimir_takhmazyan")
    .option("dbtable", "clients")
    .option("driver", "org.postgresql.Driver")
    .option("user", "vladimir_takhmazyan")
    .option("password", "R9CQBEcS")
    .save()
}
