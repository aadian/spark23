package com.kk.ln.xiada.mlib

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
  * Created by leiying on 2018/5/16.
  */
object Word2Vec {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Word2Vec").getOrCreate()
    import spark.implicits._

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)

    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)

    result.select("result").collect().foreach(println)
    """
      |[[-0.028139343485236168,0.04554025698453188,-0.013317196490243079]]
      |[[0.06872416580361979,-0.02604914902310286,0.02165239889706884]]
      |[[0.023467857390642166,0.027799883112311366,0.0331136979162693]]
    """.stripMargin




  }

}
