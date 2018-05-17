package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leiying on 2018/5/14.
  */
object StatTools {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)


    val observations = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data").map(_.split(",")).map(p => Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble))


    /*
    二、摘要统计 Summary statistics
    */
    """
      |方法名 	方法含义 	返回值类型
      |count 	列的大小 	long
      |mean 	每列的均值 	vector
      |variance 	每列的方差 	vector
      |max 	每列的最大值 	vector
      |min 	每列的最小值 	vector
      |normL1 	每列的L1范数 	vector
      |normL2 	每列的L2范数 	vector
      |numNonzeros 	每列非零向量的个数 	vector
    """.stripMargin
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("summary.count:" + summary.count)
    println("summary.mean:" + summary.mean)
    println("summary.variance:" + summary.variance)
    println("summary.max:" + summary.max)
    println("summary.min:" + summary.min)
    println("summary.normL1:" + summary.normL1)
    println("summary.normL2:" + summary.normL2)
    println("summary.numNonzeros:" + summary.numNonzeros)
    """
      |summary.count:150
      |summary.mean:[5.843333333333333,3.053999999999999,3.758666666666667,1.1986666666666665]
      |summary.variance:[0.6856935123042518,0.1880040268456377,3.113179418344516,0.5824143176733783]
      |summary.max:[7.9,4.4,6.9,2.5]
      |summary.min:[4.3,2.0,1.0,0.1]
      |summary.normL1:[876.5000000000002,458.10000000000014,563.8000000000004,179.8000000000001]
      |summary.normL2:[72.27620631992245,37.776315331170125,50.823223038292255,17.38677658451963]
      |summary.numNonzeros:[150.0,150.0,150.0,150.0]
    """.stripMargin


    /*
    三、相关性Correlations
     */

    val serialX = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data").map(_.split(",")).map(p => p(0).toDouble)
    val serialY = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data").map(_.split(",")).map(p => p(1).toDouble)

    val correlation: Double = Statistics.corr(serialX, serialY, "pearson")
    println("correlation:" + correlation)
    """
      |correlation:-0.10936924995062468
    """.stripMargin


    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data").map(_.split(",")).map(p => Vectors.dense(p(0).toDouble, p(1).toDouble))
    val corrMatrix: Matrix = Statistics.corr(data, "pearson")
    println(corrMatrix)
    """
      |1.0                   -0.10936924995062468
      |-0.10936924995062468  1.0
    """.stripMargin

    /**
      * 四、分层抽样 Stratified sampling
      */

      """
        |参数 	含义
        |withReplacement 	每次抽样是否有放回
        |fractions 	控制不同key的抽样率
        |seed 	随机数种子
      """.stripMargin

    /*
    （一）sampleByKey 方法
     */
    val dataName = sc.makeRDD(Array(
      ("female", "Lily"),
      ("female", "Lucy"),
      ("female", "Emily"),
      ("female", "Kate"),
      ("female", "Alice"),
      ("male", "Tom"),
      ("male", "Roy"),
      ("male", "David"),
      ("male", "Frank"),
      ("male", "Jack")))

    val fractions:Map[String,Double] = Map("female" -> 0.6,"male" -> 0.4)
    val approxSample = dataName.sampleByKey(withReplacement = false,fractions,1)
    approxSample.collect().foreach(println)
    """
      |(female,Lily)
      |(female,Lucy)
      |(female,Emily)
      |(female,Kate)
      |(male,Roy)
      |(male,Frank)
    """.stripMargin


    /*
    （二）sampleByKeyExact 方法
     */
    val exactSample = dataName.sampleByKeyExact(withReplacement = false, fractions, 1)
    exactSample.collect().foreach(println)
    """
      |(female,Lily)
      |(female,Emily)
      |(female,Kate)
      |(male,Roy)
      |(male,Frank)
    """.stripMargin








  }

}
