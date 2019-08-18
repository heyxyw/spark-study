package com.zhouq.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/8/18
  *
  */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BroadcastVariable").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val factorBroadcast: Broadcast[Int] = sc.broadcast(3)

    val numberList = List(1, 2, 3, 4, 6)

    val numbers: RDD[Int] = sc.parallelize(numberList, 1)

    val multipleNumbers: RDD[Int] = numbers.map(num => {
      num * factorBroadcast.value
    })

    multipleNumbers.foreach(println)

    sc.stop()


  }

}
