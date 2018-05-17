package com.kk.ln.xiada.mlib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector

/**
  * Created by leiying on 2018/5/16.
  */
object MLPipline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local").
      appName("demo").
      getOrCreate()


    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    import spark.implicits._

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    val model = pipeline.fit(training)


    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark a"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test).
      select("id", "text", "probability", "prediction").
      collect().
      foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    """
      |(4, spark i j k) --> prob=[0.540643354485232,0.45935664551476796], prediction=0.0
      |(5, l m n) --> prob=[0.9334382627383527,0.06656173726164716], prediction=0.0
      |(6, spark a) --> prob=[0.1504143004807332,0.8495856995192668], prediction=1.0
      |(7, apache hadoop) --> prob=[0.9768636139518375,0.02313638604816238], prediction=0.0
    """.stripMargin

  }

}
