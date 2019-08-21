package com.zhouq.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Accumulator
  *
  * @Author: zhouq
  * @Date: 2019-08-19
  */
object AccumulatorValaible {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorValaible").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0)

    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray)

    numbers.foreach(sum.add(_))

    println(sum)

    sc.stop()
  }

}
