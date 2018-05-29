package com.kk.ln.lab

import java.sql.Timestamp
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession


/**
  * Created by kevin on 24/5/18.
  */
object DFJoin {

  case class charge_cycle(imei: String, timeIn: String, cycleNo: Int, timeStart: String, timeEnd: String)

  case class charge_curve(imei: String, timeIn: String, serial: Long, currentDayCycleNo: Int)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("DF Join Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val cycleList = List(

      charge_cycle("A", "2018-01-01", 1, "2018-01-01 12:00:00", "2018-01-01 12:00:15"),
      charge_cycle("A", "2018-01-01", 2, "2018-01-01 15:00:00", "2018-01-01 12:00:10"),
      charge_cycle("A", "2018-01-02", 3, "2018-01-02 08:00:00", "2018-01-02 08:00:05"),

      charge_cycle("B", "2018-01-01", 1, "2018-01-01 08:00:00", "2018-01-01 08:00:05"),
      charge_cycle("B", "2018-01-02", 2, "2018-01-02 08:00:00", "2018-01-02 08:00:05")
    )

    val curveList = List(
      charge_curve("A", "2018-01-04 12:00:00", 1, 1),
      charge_curve("A", "2018-01-04 12:00:05", 2, 1),
      charge_curve("A", "2018-01-04 12:00:10", 3, 1),
      charge_curve("A", "2018-01-04 12:00:15", 4, 1),

      charge_curve("B", "2018-01-04 15:00:00", 1, 1),
      charge_curve("B", "2018-01-04 15:00:05", 2, 1),
      charge_curve("B", "2018-01-04 15:00:10", 3, 1),

      charge_curve("B", "2018-01-04 18:00:00", 1, 2),
      charge_curve("B", "2018-01-04 18:00:05", 2, 2),
      charge_curve("B", "2018-01-04 18:00:10", 3, 2),

      charge_curve("C", "2018-01-04 18:00:00", 1, 1),
      charge_curve("C", "2018-01-04 18:00:05", 2, 1),
      charge_curve("C", "2018-01-04 18:00:10", 3, 1)

    )


    val cycleDF = spark.createDataFrame(cycleList)
    cycleDF.createOrReplaceTempView("cycle")
    val sqlCycleDF = spark.sql("SELECT max(cycleNo) as maxCycleNo,imei FROM cycle GROUP BY imei")
    sqlCycleDF.createOrReplaceTempView("maxCycle")
    sqlCycleDF.show()

    val curveDF = spark.createDataFrame(curveList)
    curveDF.createOrReplaceTempView("curve")
    curveDF.show()


    val sqlJoinDF = spark.sql("SELECT (IFNULL(cc.maxCycleNo,0) + cu.currentDayCycleNo) as cycleNo, cu.*   FROM curve cu LEFT JOIN maxCycle cc ON cc.imei = cu.imei")
    sqlJoinDF.show()





    //cycleDF.createOrReplaceGlobalTempView("cycle")

    //val maxCycleNo = spark.sql("SELECT * FROM cycle")

    //maxCycleNo.show()

  }

}
