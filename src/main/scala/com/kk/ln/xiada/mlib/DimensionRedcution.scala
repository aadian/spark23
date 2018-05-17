package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leiying on 2018/5/14.
  */
object DimensionRedcution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\spark\\spark23\\src\\main\\resources\\a.mat").map(_.split(" ").map(_.toDouble)).map(line => Vectors.dense(line))

    /*
    一、奇异值分解（SVD）
     */

    //通过RDD[Vectors]创建行矩阵
    val rm = new RowMatrix(data)

    //保留前3个奇异值
    val svd = rm.computeSVD(3,computeU = true)

    println(svd.s)
    """
      |[28.741265581939565,10.847941223452608,7.089519467626695]
    """.stripMargin
    println(svd.V)
    """
      |-0.32908987300830383  0.6309429972945555    0.16077051991193514
      |-0.2208243332000108   -0.1315794105679425   -0.2368641953308101
      |-0.35540818799208057  0.39958899365222394   -0.147099615168733
      |-0.37221718676772064  0.2541945113699779    -0.25918656625268804
      |-0.3499773046239524   -0.24670052066546988  -0.34607608172732196
      |-0.21080978995485605  0.036424486072344636  0.7867152486535043
      |-0.38111806017302313  -0.1925222521055529   -0.09403561250768909
      |-0.32751631238613577  -0.3056795887065441   0.09922623079118417
      |-0.3982876638452927   -0.40941282445850646  0.26805622896042314
    """.stripMargin

    println(svd.U)
    """
      |null
    """.stripMargin

    /*
    如果需要获得U成员，可以在进行SVD分解时，指定computeU参数，令其等于True，即可在分解后的svd对象中拿到U成员，如下文所示
    val svd = rm.computeSVD(3, computeU = true)
     */


    /*
    二、主成分分析（PCA）
     */

    //保留前3个主成分
    val pc = rm.computePrincipalComponents(3)
    println(pc)





  }


}
