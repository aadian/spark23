package com.kk.ln.databricks.StructuredStreamETL

import org.apache.spark.sql.SparkSession

object IRQuery {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val parquetOutputPath = "E:\\spark\\spark23\\src\\main\\resources\\cloudtrail"
    val parquetData = spark.sql(s"select userName, count(*), row_number() OVER (PARTITION BY userName order by count(*)) rank from parquet.`$parquetOutputPath` group by userName")
    parquetData.show()


  }

}
