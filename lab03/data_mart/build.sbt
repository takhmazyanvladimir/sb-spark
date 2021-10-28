name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"

libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "7.15.0"
