name := "data_mart"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("org.example")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.postgresql" %% "postgresql" % "42.1.4"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector_2.11" % "2.5.1"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20_2.11" % "7.15.0"
