package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by leiying on 2018/5/15.
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data")

    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(
        if (parts(4) == "Iris-setosa") 0.toDouble else if (parts(4) == "Iris-versicolor") 1.toDouble else 2.toDouble,
        Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble,parts(3).toDouble)
      )
    }

    parsedData.foreach { x => println(x) }

    val splits = parsedData.randomSplit(Array(0.6, 0.4), 11L)

    val training = splits(0).cache()

    val test = splits(1)

    val model = new LogisticRegressionWithLBFGS().
      setNumClasses(3).
      run(training)

    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    predictionAndLabels.foreach(println)

    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("precision:" + precision)
    """
      |precision:0.9615384615384616
    """.stripMargin





  }

}
