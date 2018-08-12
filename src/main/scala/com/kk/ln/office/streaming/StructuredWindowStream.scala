package com.kk.ln.office.streaming

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

/**
  * Created by leiying on 2018/5/23.
  */
object StructuredWindowStream {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Structured Window Stream").getOrCreate()
    val sc = spark.sparkContext.setLogLevel("WARN")

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    import spark.implicits._
    val words = lines.as[(String,Timestamp)].flatMap( line =>
      line._1.split(" ").map( word => (word, line._2))
    ).toDF("word","timestamp")

    val windowedCounts = words.groupBy(
      window($"timestamp", "2 minutes", "1 minutes"),
      $"word"
    ).count()

    val query = windowedCounts
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }

}
