package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leiying on 2018/5/15.
  */
object DecisionTreeRegression {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data")

    var parseData = data.map { line =>
      var parts = line.split(',')
      LabeledPoint(if (parts(4) ==
        "Iris-setosa") 0.toDouble
      else if (parts(4) ==
        "Iris-versicolor") 1.toDouble
      else 2.toDouble
        , Vectors.dense(parts(0).toDouble)
      )
    }

    parseData.foreach(println)

    val splits = parseData.randomSplit(Array(0.7, 0.3), 11L)
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDeep = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDeep, maxBins)

    val labelAndPrediction = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    println("Learned classification tree model:\n" + model.toDebugString)

    val testErr = labelAndPrediction.filter(r => r._1 != r._2).count().toDouble / testData.count()

    println("Precision = " + (1-testErr))
  }

}
