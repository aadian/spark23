package com.kk.ln.office.sql



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WindowsFunc {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("windows funcitons")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._


    val df = Seq((1, "a"), (1, "a"), (2, "a"),(5, "a"),(3, "a"), (1, "b"), (2, "b"), (3, "b"))
      .toDF("id", "category")

    df.show()


    val byCategoryOrderedById =
      Window.partitionBy("category").orderBy("id").rowsBetween(Window.currentRow, 1)
    df.withColumn("sum", sum($"id").over(byCategoryOrderedById)).show()
    //df.withColumn("sum", sum("id") ).show()


  }

}
