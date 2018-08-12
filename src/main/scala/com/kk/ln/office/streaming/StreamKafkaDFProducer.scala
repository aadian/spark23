package com.kk.ln.office.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamKafkaDFProducer {

  case class keyValue(key: String, value: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("Stream Kafka DF producer").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val keyValueList = List(
      keyValue("A", 1),
      keyValue("A", 2))

    //val df:DataFrame = null

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()


    val query = df
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test1")
      .option("checkpointLocation", "E:\\spark\\spark23\\src\\main\\resources\\")
      .start()


  }

}
