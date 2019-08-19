package com.zhouq.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/8/14
  *
  */
object ActionOperation {

  def main(args: Array[String]): Unit = {
//    countByKey()
    collect()
  }

  def collect():Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)

    val numbersRDD = sc.parallelize(numbers)

    val doubleNumbersRDD = numbersRDD.map( _ * 2)
    val numbersArrays = doubleNumbersRDD.collect()

    numbersArrays.foreach(println)
    sc.stop()

  }


  def countByKey(): Unit = {
    val conf = new SparkConf().setAppName("countByKey").setMaster("local")

    val sc = new SparkContext(conf)

    val sourceArray = Array(Tuple2("class1", "ls"), Tuple2("class1", "ww"), Tuple2("class2", "mz"))


    val sourceRDD: RDD[(String, String)] = sc.parallelize(sourceArray)

    val countBySource: collection.Map[String, Long] = sourceRDD.countByKey()

    print(countBySource)
//    countBySource.foreach(println)
    sc.stop()
  }
}
