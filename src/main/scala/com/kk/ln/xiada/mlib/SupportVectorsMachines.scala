package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

/**
  * Created by leiying on 2018/5/15.
  */
object SupportVectorsMachines {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)

    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data")
    val parseData = data.map { line =>
      var parts = line.split(',')
      LabeledPoint(
        if (parts(0) == "Iris-setosa") 0.toDouble else if (parts(4) == "Iris-versicolor") 1.toDouble else 2.toDouble,
        Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble))
    }

    val splits = parseData.filter(point => point.label != 2).randomSplit(Array(0.6, 0.4), 11L)
    val (training, test) = (splits(0).cache(), splits(1))
    val numIterations = 1000

    val model = SVMWithSGD.train(training, numIterations)

    // 接下来，我们清除默认阈值，这样会输出原始的预测评分，即带有确信度的结果。
    model.clearThreshold()

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    scoreAndLabels.foreach(println)
    """
      |(47.54834375930158,1.0)
      |(53.852782676664745,1.0)
      |(49.28106315447772,1.0)
      |(53.05016631987218,1.0)
      |(49.60907303545002,1.0)
    """.stripMargin

    //那如果设置了阈值，则会把大于阈值的结果当成正预测，小于阈值的结果当成负预测。
    model.setThreshold(0.0)
    scoreAndLabels.foreach(println)
    """
      |(1.0,1.0)
      |(1.0,1.0)
      |(1.0,1.0)
      |(1.0,1.0)
      |(1.0,1.0)
    """.stripMargin

    //构建评估矩阵，把模型预测的准确性打印出来
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)
    """
      |Area under ROC = 1.0
    """.stripMargin


    val svmAlg = new SVMWithSGD()

    svmAlg.optimizer.
      setNumIterations(20).
      setRegParam(0.1).
      setUpdater(new L1Updater)

    val modelL1 = svmAlg.run(training)


  }

}
