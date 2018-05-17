package com.kk.ln.xiada.mlib

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by leiying on 2018/5/16.
  */
object KMeans {

  case class model_instance(features: Vector)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("KMeans").getOrCreate()
    val sc = spark.sparkContext

    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\iris.data")

    //开启RDD的隐式转换
    import spark.implicits._

    val df = data.map(line => {
      model_instance( Vectors.dense(line.split(",").filter(p => p.matches("\\d*(\\.?)\\d*")).map(_.toDouble)) )
    }).toDF()


    val kmeansmodel = new KMeans().setK(3).setFeaturesCol("features").setPredictionCol("prediction").fit(df)
    val resluts = kmeansmodel.transform(df)

    resluts.collect().foreach(
      row => println(row(0) + " is clustuer of " + row(1))
    )
    """
      |[6.7,3.0,5.2,2.3] is clustuer of 2
      |[6.3,2.5,5.0,1.9] is clustuer of 0
      |[6.5,3.0,5.2,2.0] is clustuer of 2
      |[6.2,3.4,5.4,2.3] is clustuer of 2
      |[5.9,3.0,5.1,1.8] is clustuer of 0
    """.stripMargin


    kmeansmodel.clusterCenters.foreach(
      center => println("Clustering Center:" + center)
    )
    """
      |通过KMeansModel类自带的clusterCenters属性获取到模型的所有聚类中心情况
      |Clustering Center:[5.88360655737705,2.7409836065573776,4.388524590163936,1.4344262295081969]
      |Clustering Center:[5.005999999999999,3.4180000000000006,1.4640000000000002,0.2439999999999999]
      |Clustering Center:[6.853846153846153,3.0769230769230766,5.715384615384615,2.053846153846153]
    """.stripMargin



    println(kmeansmodel.computeCost(df))
    """
      |KMeansModel类也提供了计算 集合内误差平方和（Within Set Sum of Squared Error, WSSSE)
      |的方法来度量聚类的有效性，在真实K值未知的情况下，该值的变化可以作为选取合适K值的一个重要参考
      |78.94506582597637
    """.stripMargin


  }


}
