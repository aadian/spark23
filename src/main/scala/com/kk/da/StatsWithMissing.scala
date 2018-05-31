package com.kk.da

import org.apache.spark.util.StatCounter

/**
  * Created by kevin on 20/5/18.
  */

object TestNAStatCounter {


  def main(args: Array[String]) {
    val nas1 = new NAStatCounter()
    nas1.add(1.0)
    nas1.add(8.0)
    nas1.add(Double.NaN)
    println(nas1.toString)

    val nas2 = new NAStatCounter()
    nas2.add(2.34)
    nas2.add(Double.NaN)

    nas1.merge(nas2)
    println(nas1.toString)

    val arr = Array(12.5, Double.NaN, 69.0)
    val nas = arr.map(x => NAStatCounter(x))

  }

}


class NAStatCounter extends Serializable {

  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x))
      missing += 1
    else
      stats.merge(x)

    this

  }

  def merge(other: NAStatCounter): NAStatCounter = {
    missing += other.missing
    stats.merge(other.stats)

    this

  }

  override def toString = {
    "stats: " + stats.toString() + " NaN: " + missing
  }


}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}





