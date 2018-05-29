package com.kk.da

import org.apache.spark.sql.SparkSession

/**
  * Created by kevin on 19/5/18.
  */
object DataAnaylse {


  //case class 是不可变的一种简单类型，内置了所有java类的基本方法，可以通过名字来访问
  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("Data Anaylse").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val rawblocks = sc.textFile("/Develop/projects/spark23/src/main/resources/linkage/block_1.csv")

    println(rawblocks.getClass.getTypeName)
    """
      |org.apache.spark.rdd.MapPartitionsRDD
    """.stripMargin


    val first = rawblocks.first()
    println(first.getClass.getTypeName)
    """
      |java.lang.String
    """.stripMargin

    val head = rawblocks.take(10)
    println(head.getClass.getTypeName)
    """
      |java.lang.String[]
    """.stripMargin
    //rawblocks.foreach(line => println(line))

    head.filter(isHead).foreach(println)
    """
      |"id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"
    """.stripMargin

    println(head.filterNot(isHead).length)
    """
      |9
    """.stripMargin

    val lines = head.filter(!isHead(_))
    println(lines.length)
    """
      |9
    """.stripMargin


    val mds = head.filter(x => !isHead(x)).map(x => parse(x))
    val noheader = rawblocks.filter(x => !isHead(x))
    val parsed = noheader.map(line => parse(line))
    println(mds.length)
    println(parsed.getClass.getTypeName)

    /**
      * 聚集,在聚集前过滤掉经可能多的数据，尽快得到答案
      */

    val grouped = mds.groupBy(md => md.matched)
    grouped.mapValues(x => x.size).foreach(println)
    grouped.keys.foreach(println)

    val matchCounts = parsed.map(md => md.matched).countByValue()
    val matchedCountSeq = matchCounts.toSeq

    //按第一个字段排，默认升序
    matchedCountSeq.sortBy(_._1).foreach(println)
    """
      |(false,572820)
      |(true,2093)
    """.stripMargin

    //按第二个字段排，默认升序
    matchedCountSeq.sortBy(_._2).foreach(println)
    """
      |(true,2093)
      |(false,572820)
    """.stripMargin


    //按第一个字段排，降序
    matchedCountSeq.sortBy(_._1).reverse.foreach(println)
    """
      |(true,2093)
      |(false,572820)
    """.stripMargin


    val nas1 = new NAStatCounter()
    nas1.add(1.0)
    nas1.add(8.0)
    nas1.add(Double.NaN)
    println(nas1.toString)
    """
      |stats: (count: 2, mean: 4.500000, stdev: 3.500000, max: 8.000000, min: 1.000000) NaN: 1
    """.stripMargin

    val nas2 = new NAStatCounter()
    nas2.add(2.34)
    nas2.add(Double.NaN)

    nas1.merge(nas2)
    println(nas1.toString)
    """
      |stats: (count: 3, mean: 3.780000, stdev: 3.033722, max: 8.000000, min: 1.000000) NaN: 2
    """.stripMargin

    val arr = Array(12.5, Double.NaN, 69.0)
    val nas = arr.map(x => NAStatCounter(x))
    println(nas.toString)
    """
      |stats: (count: 2, mean: 40.750000, stdev: 28.250000, max: 69.000000, min: 12.500000) NaN: 1
    """.stripMargin

    val nasRDD = parsed.map(md => {
      md.scores.map(x => NAStatCounter(x))
    })

    //使用zip将数组组合
    val nas3 = Array(10.0, Double.NaN, 8.9).map(x => NAStatCounter(x))
    val nas4 = Array(45.9, Double.NaN, 1, 9.0).map(x => NAStatCounter(x))
    val merged = nas3.zip(nas4).map(p => p._1.merge(p._2))
    println(merged.toString)

    val merged2 = nas3.zip(nas4).map { case (a, b) => a.merge(b) }

    val nasn = List(nas3, nas4)
    val mergedn = nasn.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })


  }

  def isHead(line: String) = {
    line.contains("id_1")
  }

  def parse(line: String) = {
    val pieces = line.split(',')
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }

  def toDouble(field: String) = {
    if ("?".equals(field)) Double.NaN else field.toDouble
  }


}
