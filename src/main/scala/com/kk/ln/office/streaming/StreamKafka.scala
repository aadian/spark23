package com.kk.ln.office.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object StreamKafka {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("kafka stream")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    /**
      * Streaming Queries
      * Subscribe to 1 topic
      */
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "test")
//      .load()

    /**
      *  Batch Queries
      */
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = ds.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }


}
