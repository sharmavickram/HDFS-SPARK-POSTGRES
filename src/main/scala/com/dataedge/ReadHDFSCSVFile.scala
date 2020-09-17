package com.dataedge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ReadHDFSCSVFile {
  def main(args: Array[String]): Unit = {

    val spark_conf = new SparkConf().setAppName("READ-CSV").setMaster("local[1]")
    val sc = new SparkContext(spark_conf)

    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    //CSV file location on HDFS
    val hdfs_file_location = "hdfs://localhost:9000/user/data/csv/emp_data.csv"

    //Reading from HDFS
    val username_df = readFromHDFS(hdfs_file_location, spark_session)

    username_df.printSchema()
    username_df.collect().foreach(println)

  }

  def readFromHDFS(file_location: String, spark_session: SparkSession): DataFrame ={

    val username_df = spark_session.read.format("csv")
      .option("delimiter",";")
      .option("header","true")
      .option("inferSchema", "true")
      .load(file_location)

    username_df

  }

}
