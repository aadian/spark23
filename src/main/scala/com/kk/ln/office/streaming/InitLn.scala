package com.kk.ln.office.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by leiying on 2018/5/21.
  */
object InitLn {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))

    //CalSocketStream(ssc)
    CalFileStream(ssc)


    ssc.start()
    ssc.awaitTermination()
  }

  private def CalSocketStream(ssc: StreamingContext) = {
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
  }

  private def CalFileStream(ssc: StreamingContext) = {
    val lines = ssc.textFileStream("E:\\spark\\spark23\\src\\main\\resources\\stream")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
  }
}
