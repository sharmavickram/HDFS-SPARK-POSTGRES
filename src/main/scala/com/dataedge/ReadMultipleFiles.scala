package com.dataedge

import org.apache.spark.sql.SparkSession

object ReadMultipleFiles {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("read all text files from a directory to single RDD")
    val rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/data/txt/*")
    rdd.foreach(f => {
      println(f)
    })

    println("read text files base on wildcard character")
    val rdd2 = spark.sparkContext.textFile("hdfs://localhost:9000/user/data/txt/text*.txt")
    rdd2.foreach(f => {
      println(f)
    })

    println("read multiple text files into a RDD")
    val rdd3 = spark.sparkContext.textFile("hdfs://localhost:9000/user/data/txt/text01.txt,hdfs://localhost:9000/user/data/txt/text03.txt")
    rdd3.foreach(f => {
      println(f)
    })

    println("Read files and directory together")
    val rdd4 = spark.sparkContext.textFile("R:\\hadoop\\data\\txt\\text01.txt,R:\\hadoop\\data\\txt\\text02.txt,R:\\hadoop\\data\\txt\\*")
    rdd4.foreach(f => {
      println(f)
    })


    val rddWhole = spark.sparkContext.wholeTextFiles("hdfs://localhost:9000/user/data/txt/*")
    rddWhole.foreach(f => {
      println(f._1 + "=>" + f._2)
    })

    val rdd5 = spark.sparkContext.textFile("hdfs://localhost:9000/user/data/txt/*")
    val rdd6 = rdd5.map(f => {
      f.split(",")
    })

    rdd6.foreach(f => {
      println("Col1:" + f(0) + ",Col2:" + f(1))
    })

  }

}
