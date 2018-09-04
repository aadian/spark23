package com.kk.office.stream

import org.apache.spark.sql.SparkSession

/**
  * Created by kevin on 22/5/18.
  */
object StructedStream {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("structed stream").getOrCreate()

    val sc = spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    //println(lines.getClass.getTypeName)

    val words = lines.as[String].flatMap(_.split(" "))
    //println(words.getClass.getTypeName)

    val wordCounts = words.groupBy("value").count()
    //println(wordCounts.getClass.getTypeName)

    val query = words.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
