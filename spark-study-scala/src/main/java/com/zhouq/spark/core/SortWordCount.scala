package com.zhouq.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * //todo
  *
  * @Author: zhouq
  * @Date: 2019-08-19
  */
object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount1").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/zhouqiao/Desktop/work/ideaproject/spark-study/spark-study-java/src/main/java/com/zhouq/spark/core/AccumulatorValaible.java")

    lines.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .map(word =>{(word._2,word._1)})
      .sortByKey(false)
      .map(word =>{(word._2,word._1)})
      .foreach(println)

    sc.stop()
  }

}
