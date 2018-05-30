package com.kk.office.stream

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * Created by kevin on 30/5/18.
  */
object KafkaIntegration {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[2]").appName("kafka integration").getOrCreate()

    import spark.implicits._

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("subscribe", "my-replicated-topic")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]




    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    query.awaitTermination()
  }

}
