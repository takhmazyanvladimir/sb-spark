name := "data_mart"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("org.example")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "7.15.0"
