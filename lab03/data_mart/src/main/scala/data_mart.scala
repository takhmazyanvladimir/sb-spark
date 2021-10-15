package org.example

import org.apache.spark.sql.SparkSession

object data_mart extends  App{
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sql("select 1").show()

}
