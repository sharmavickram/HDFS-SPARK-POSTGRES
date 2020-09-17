name := "HDFS-SPARK-POSTGRES"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// https://mvnrepository.com/artifact/postgresql/postgresql
libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"


libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.4"
