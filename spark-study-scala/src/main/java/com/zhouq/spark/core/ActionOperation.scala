package com.zhouq.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/8/14
  *
  */
object ActionOperation {

  def main(args: Array[String]): Unit = {
    countByKey()
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
