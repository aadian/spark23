package com.kk.ln.xiada.mlib

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by leiying on 2018/5/16.
  */
object TFIDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("TFIDF").getOrCreate()
    import spark.implicits._

    val sentenceData = spark.createDataFrame(Seq(
      (0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordsData = tokenizer.transform(sentenceData)

    wordsData.show(false)
    """
      |+-----+------------------------------------+---------------------------------------------+
      ||label|sentence                            |words                                        |
      |+-----+------------------------------------+---------------------------------------------+
      ||0    |I heard about Spark and I love Spark|[i, heard, about, spark, and, i, love, spark]|
      ||0    |I wish Java could use case classes  |[i, wish, java, could, use, case, classes]   |
      ||1    |Logistic regression models are neat |[logistic, regression, models, are, neat]    |
      |+-----+------------------------------------+---------------------------------------------+
    """.stripMargin

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.select("rawFeatures").show(false)
    """
      |+---------------------------------------------------------------------+
      ||rawFeatures                                                          |
      |+---------------------------------------------------------------------+
      ||(2000,[240,333,1105,1329,1357,1777],[1.0,1.0,2.0,2.0,1.0,1.0])       |
      ||(2000,[213,342,489,495,1329,1809,1967],[1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
      ||(2000,[286,695,1138,1193,1604],[1.0,1.0,1.0,1.0,1.0])                |
      |+---------------------------------------------------------------------+
    """.stripMargin

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("features","label").take(3).foreach(println)
    """
      |[(2000,[240,333,1105,1329,1357,1777],[0.6931471805599453,0.6931471805599453,1.3862943611198906,0.5753641449035617,0.6931471805599453,0.6931471805599453]),0]
      |[(2000,[213,342,489,495,1329,1809,1967],[0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.28768207245178085,0.6931471805599453,0.6931471805599453]),0]
      |[(2000,[286,695,1138,1193,1604],[0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453]),1]
    """.stripMargin




  }



}
