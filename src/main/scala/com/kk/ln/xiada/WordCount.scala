package com.kk.ln.xiada

import org.apache.spark.sql.SparkSession

/**
  * Created by leiying on 2018/5/17.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Word Count").getOrCreate()
    val lines = spark.sparkContext.textFile("/Develop/projects/spark23/target/classes/wordCountText.txt")
    val wordsMap = scala.collection.mutable.Map[String, Int]()
    wordsMap += ("this" -> 1)
    lines.foreach(line =>
      line.split(" ").foreach(
        word => {
          print(word)
          if (wordsMap.contains(word)) {
            wordsMap(word) += 1
          } else {
            wordsMap += (word -> 1)
          }
        }
      )
    )
    println(wordsMap.size)
    for ((k, v) <- wordsMap) println(k + ":" + v)


  }

}
