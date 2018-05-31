package com.kk.ln.lab


import com.kk.ln.lab.WindowFun.battery_data
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by kevin on 29/5/18.
  */
object WindowFun {

  case class battery_data(imei: String, timeIn: String, volt: Int)

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Study Window Function")
      .getOrCreate()

    import spark.implicits._

    val window = Window.partitionBy("imei").orderBy("timeIn")

    spark.sparkContext.setLogLevel("WARN")

    val batteryList = List(

      battery_data("A", "2018-01-01 12:00:00", 23),
      battery_data("A", "2018-01-01 15:00:00", 25),
      battery_data("A", "2018-01-02 08:00:00", 28),

      battery_data("B", "2018-01-01 08:00:00", 16),
      battery_data("B", "2018-01-02 08:00:05", 19),
      battery_data("B", "2018-01-02 08:00:10", 7),
      battery_data("B", "2018-01-02 08:00:15", 27)
    )

    val batteryDF = spark.createDataFrame(batteryList)

    batteryDF.createOrReplaceTempView("battery")

    val df0 = batteryDF.withColumn("voltUp", lag(batteryDF.col("volt"), 1, 0).over(window))
    df0.show()

    //val sqlDF = spark.sql("select imei,timeIn,row_number() over (partition by imei order by timeIn desc) as r from battery")
    //sqlDF.show()


  }


}
