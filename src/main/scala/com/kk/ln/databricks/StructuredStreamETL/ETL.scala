package com.kk.ln.databricks.StructuredStreamETL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

object ETL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    /**
      * schema:
      * Records
      * userid: 001
      * username: Jim Zhang
      * loginTimes 1527830168
      *
      * userid: 002
      * *   username: Rose Liu
      * *   loginTimes 1527830168
      * ...
      */
    //    val logSchema = new StructType()
    //      .add("Records", ArrayType(new StructType()
    //        .add("userId", StringType)
    //        .add("userName", new StructType()
    //          .add("firstName", StringType)
    //          .add("lastName", StringType))
    //        .add("loginTime", StringType)))
    val logSchema = new StructType()
      .add("Records", ArrayType(new StructType()
        .add("userId", StringType)
        .add("userName", new StructType()
          .add("firstName", StringType)
          .add("lastName", StringType))
        .add("loginTime", StringType)
      ))


    val logPath = "E:\\spark\\spark23\\src\\main\\resources\\logs\\"
    val parquetOutputPath = "E:\\spark\\spark23\\src\\main\\resources\\cloudtrail"
    val checkPointPath = "E:\\spark\\spark23\\src\\main\\resources\\checkpoint"

    val rawRecords = spark.readStream
      .option("maxFilesPerTrigger", "100")
      .schema(logSchema)
      .json(logPath)

    //    val cloudTrailEvents = rawRecords
    //      .select(explode($"Records") as "record")
    //      .select(
    //        unix_timestamp($"record.loginTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "loginTime",
    //        $"record.userName")

    val cloudTrailEvents = rawRecords
      .select(explode($"Records") as "record")
      .select(
        $"record.userName",
        unix_timestamp($"record.loginTime", "yyyy-MM-dd hh:mm:ss").cast("timestamp") as "loginTime"
      )

    //.select(unix_timestamp($"record.loginTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "loginTime", "record.userName")

    //
    //    val checkPointPath = "E:\\spark\\spark23\\src\\main\\resources\\checkpoint"
    //    val streamingELTQuery = cloudTrailEvents
    //      .writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()


    val streamingELTQuery = cloudTrailEvents.withColumn("date", $"loginTime".cast("date"))
      .writeStream
      .format("parquet")
      .option("path", parquetOutputPath)
      .partitionBy("date")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", checkPointPath)
      .start()


    streamingELTQuery.awaitTermination()


  }

}
