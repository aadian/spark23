package com.kk.ln.databricks.StructuredStreamETL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object Query {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val parquetOutputPath = "E:\\spark\\spark23\\src\\main\\resources\\cloudtrail"
    val parquetData = spark.sql(s"select * from parquet.`$parquetOutputPath`")
    parquetData.show()


  }

}
