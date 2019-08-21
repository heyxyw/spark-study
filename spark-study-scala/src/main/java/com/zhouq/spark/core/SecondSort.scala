package com.zhouq.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * //todo
  *
  * @Author: zhouq
  * @Date: 2019-08-20
  */
object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/zhouqiao/dev/test/sparktestdata/sort.txt")

    lines.map(line => {
      (new SecondSortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt), line)
    })
      .sortByKey()
      .map(sortedPair => sortedPair._2)
      .foreach(println)

    sc.stop()
  }

}
