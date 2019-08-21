package com.zhouq.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/8/20
  *
  */
object Top3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("G:\\stady\\scalatest\\number.txt")

    val pair: RDD[(Int, String)] = lines.map(line => (line.toInt,line))

    val sourtedPair: RDD[(Int, String)] = pair.sortByKey(false)
    val sortedNumbers: RDD[Int] = sourtedPair.map(_._1)
    val top3Numbers: Array[Int] = sortedNumbers.take(3)
    for( number <- top3Numbers){
      println(number)
    }

    sc.stop()

  }

}
