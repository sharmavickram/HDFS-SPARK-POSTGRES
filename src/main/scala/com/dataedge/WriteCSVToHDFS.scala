package com.dataedge

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object WriteCSVToHDFS {
  def main(args: Array[String]): Unit = {

    val spark_conf = new SparkConf().setAppName("READ-CSV").setMaster("local[1]")
    val sc = new SparkContext(spark_conf)

    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    //CSV file location on HDFS
    val hdfs_file_location = "hdfs://localhost:9000/user/data/csv/emp_data.csv"

    //Postgres jdbc connection URL
    val postgres_url = "jdbc:postgresql://localhost:5432/postgres"

    //Reading from HDFS
    val username_df = readFromHDFS(hdfs_file_location, spark_session)

    //Writing to PostgreSQL
    writeToPostgreSQL(postgres_url, spark_session, username_df)


  }
  def readFromHDFS(file_location: String, spark_session: SparkSession): DataFrame ={

    val username_df = spark_session.read.format("csv")
      .option("delimiter",";")
      .option("header","true")
      .option("inferSchema", "true")
      .load(file_location)

    username_df

  }
  def writeToPostgreSQL(postgres_url: String , spark_session: SparkSession, username_df: DataFrame): Unit = {

    val connection_props = new Properties()
    connection_props.setProperty("driver", "org.postgresql.Driver")
    connection_props.setProperty("user", "postgres")
    connection_props.setProperty("password", "postgres")

    val table_name = "public.Username"

    //Passing in the URL, table in which data will be written and relevant connection properties
    username_df.write.mode(SaveMode.Append).jdbc(postgres_url,table_name,connection_props)

  }

}
