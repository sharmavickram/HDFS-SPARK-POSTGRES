package com.dataedge

object ReadJSONFile {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder.master("local[1]").appName("ReadJSONFile").getOrCreate()

    val jsonDF = spark.read.json("hdfs://localhost:9000/user/data/json/part-r-00000.json")
    jsonDF.printSchema()  // print the schema of json file

    jsonDF.show()  //print content

    ///jsonDF.write.option("compression","snappy").format("avro").save("hdfs://localhost:9000/user/data/output/json-avro-snappy.avro")

    val verify_avro_data = spark.read.format("avro").load("hdfs://localhost:9000/user/data/output/json-avro-snappy.avro/part-00000-a0854fdb-b390-4a3d-8bdc-89658ad445ba-c000.avro")
    verify_avro_data.show()

  }

}
