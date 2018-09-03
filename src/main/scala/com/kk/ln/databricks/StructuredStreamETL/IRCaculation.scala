package com.kk.ln.databricks.StructuredStreamETL

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object IRCaculation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("IR Caculation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    /**
      * schema:
      * Records
      * imei: 001
      * timeIn:
      * cellVolt1:3.6
      * cellVolt2:3.6
      *
      */
    //    val batterySchema = new StructType()
    //      .add("Records", ArrayType(new StructType()
    //        .add("userId", StringType)
    //        .add("userName", new StructType()
    //          .add("firstName", StringType)
    //          .add("lastName", StringType))
    //        .add("loginTime", StringType)))
    val batterySchema = new StructType()
      .add("Records", ArrayType(new StructType()
        .add("imei", StringType)
        .add("timeIn", TimestampType)
        .add("cellVolt1", StringType)
        .add("cellVolt2", StringType)
      ))

    val logPath = "E:\\spark\\spark23\\src\\main\\resources\\IRCaculation\\logs\\"
    val parquetOutputPath = "E:\\spark\\spark23\\src\\main\\resources\\IRCaculation\\cloudtrail"
    val checkPointPath = "E:\\spark\\spark23\\src\\main\\resources\\IRCaculation\\checkpoint"

    val window = Window.partitionBy("imei").orderBy("timeIn")

    val rawRecords = spark.readStream
      .option("maxFilesPerTrigger", "100")
      .schema(batterySchema)
      .json(logPath)


    val cloudTrailEvents = rawRecords
      .select(explode($"Records") as "record")
      .select($"record.*")

    println(getStartTimeOfToday)


    //val lagRecords = cloudTrailEvents.withColumn("timeIn_lag_Down", lag(cloudTrailEvents.col("timeIn"), 1, getTimestamp("2017-01-01 12:11:11")).over(window))
    val lagRecords = cloudTrailEvents.withColumn("timeIn_lag_Down", sum($"cellVolt1").over(window))

    val streamingELTQuery = lagRecords
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    //    val streamingELTQuery = cloudTrailEvents.withColumn("date", $"loginTime".cast("date"))
    //      .writeStream
    //      .format("parquet")
    //      .option("path", parquetOutputPath)
    //      .partitionBy("date")
    //      .trigger(Trigger.ProcessingTime("10 seconds"))
    //      .option("checkpointLocation", checkPointPath)
    //      .start()

    //    val cloudTrailEvents = rawRecords
    //      .select(explode($"Records") as "record")
    //      .select(
    //        $"record.imei",
    //        unix_timestamp($"record.timeIn", "yyyy-MM-dd hh:mm:ss").cast("timestamp") as "timeIn"
    //      ).withColumn("date", $"timeIn".cast("date")).groupBy($"record.imei")

    //.select(unix_timestamp($"record.loginTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "loginTime", "record.userName")

    //
    //    val checkPointPath = "E:\\spark\\spark23\\src\\main\\resources\\checkpoint"
    //    val streamingELTQuery = cloudTrailEvents
    //      .writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()

    //    val streamingELTQuery = rawRecords
    //      .writeStream
    //      .format("console")
    //      .outputMode("update")
    //      .trigger(Trigger.ProcessingTime("10 seconds"))
    //      .start()


    //    val streamingELTQuery = cloudTrailEvents.withColumn("date", $"loginTime".cast("date"))
    //      .writeStream
    //      .format("parquet")
    //      .option("path", parquetOutputPath)
    //      .partitionBy("date")
    //      .trigger(Trigger.ProcessingTime("10 seconds"))
    //      .option("checkpointLocation", checkPointPath)
    //      .start()


    streamingELTQuery.awaitTermination()


  }

  def getStartTimeOfToday: Long = {
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val formatTime = dateFormat.format(now) // "2018-01-01"
    val date = dateFormat.parse(formatTime) //" 月 日 00:00:00 2018"
    val start = date.getTime + ""
    val startTime = start.substring(0, 10).toLong
    startTime
  }

  def getTimestamp(x: String): java.sql.Timestamp = {
    //       "20151021235349"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var ts = new Timestamp(System.currentTimeMillis());
    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    return null
  }

}
