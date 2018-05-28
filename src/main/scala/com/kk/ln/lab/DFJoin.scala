package com.kk.ln.lab

import org.apache.spark.sql.SparkSession


/**
  * Created by kevin on 25/5/18.
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
    println("充电循环数据")
    cycleDF.show()

    cycleDF.createOrReplaceTempView("cycle")
    val sqlCycleDF = spark.sql("SELECT max(cycleNo) as maxCycleNo,imei FROM cycle GROUP BY imei")
    sqlCycleDF.createOrReplaceTempView("maxCycle")
    println("每个IMEI最大充电循环号数据")
    sqlCycleDF.show()


    val curveDF = spark.createDataFrame(curveList)
    curveDF.createOrReplaceTempView("curve")
    println("当天充电曲线数据")
    curveDF.show()


    val sqlJoinDF = spark.sql("SELECT (IFNULL(cc.maxCycleNo,0) + cu.currentDayCycleNo) as cycleNo, cu.*   FROM curve cu LEFT JOIN maxCycle cc ON cc.imei = cu.imei")
    println("通过SQL关联计算当天的实际充电循环号")
    sqlJoinDF.show()


    //cycleDF.createOrReplaceGlobalTempView("cycle")

    //val maxCycleNo = spark.sql("SELECT * FROM cycle")

    //maxCycleNo.show()

  }

}
