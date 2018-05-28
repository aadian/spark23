package com.kk.ln.office.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by leiying on 2018/5/22.
  */
object StructedStream {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config(new SparkConf().setMaster("local[2]"))
      .getOrCreate()
    //val sc = spark.sparkContext.setLogLevel("WARN")


    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
    //println(wordCount.toString())


  }


}
