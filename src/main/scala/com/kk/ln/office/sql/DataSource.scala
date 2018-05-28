package com.kk.ln.office.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by leiying on 2018/5/25.
  */
object DataSource {

  val usersParquetPath = "E:\\spark\\spark23\\src\\spark-master\\examples\\src\\main\\resources\\users.parquet"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("sql data source")
      .getOrCreate()

    val userDF = spark.read.load(usersParquetPath)
    userDF.select("name","favorite_color").write.save("source\\nameAndFavColor.parquet")

    val peopleDF = spark.read.format("json").load("src\\spark-master\\examples\\src\\main\\resources\\people.json")
    peopleDF.select("name","age").write.format("parquet").save("source\\namesAndAges.parquet")




  }

}
