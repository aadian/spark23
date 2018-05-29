package com.kk.office.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by kevin on 21/5/18.
  */
object InitLn {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("stream ln")
    val ssc = new StreamingContext(conf, Seconds(1))

    //val sc = SparkSession.builder().master("local[2]").appName("stream ln").getOrCreate()
    //val ssc1 = new StreamingContext(sc,Seconds(1))

    val lines = ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" "))
    val pair = words.map( word => (word,1))
    val wordCount = pair.reduceByKey(_ + _)

    wordCount.print()



    ssc.start()
    ssc.awaitTermination()

  }

}
