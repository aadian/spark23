package com.kk.ln.lab

/**
  * Created by leiying on 2018/5/16.
  */
object Programma {

  def main(args: Array[String]): Unit = {

    val myStrArr = new Array[String](3) //声明一个长度为3的字符串数组，每个数组元素初始化为null
    myStrArr(0) = "BigData"
    myStrArr(1) = "Hadoop"
    myStrArr(2) = "Spark"
    for (i <- 0 to 2) println(myStrArr(i))
    for (i <- myStrArr) println(i)
  }

}
