package com.zhouq.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/7/24
  *
  */
object TransformationOperation {
  def main(args: Array[String]): Unit = {
    //    map()
    //    filter()
    //    flatMap()

    //    groupByKey()
//    reduceByKey()
    sortByKey()
  }

  def map(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)

    val numberRDD: RDD[Int] = sc.parallelize(numbers)

    val multipleNumberRDD: RDD[Int] = numberRDD.map(num => num * 2)

    multipleNumberRDD.foreach(println)

    sc.stop()
  }

  def filter(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numberRDD: RDD[Int] = sc.parallelize(numbers)

    val evenNumberRDD: RDD[Int] = numberRDD.filter(num => num % 2 == 0)

    evenNumberRDD.foreach(println)

    sc.stop()
  }

  def flatMap(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("flatMap").setMaster("local")

    val sc = new SparkContext(conf)

    val lineArrays = Array("i love you", "me me da")

    val lines: RDD[String] = sc.parallelize(lineArrays)

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    words.foreach(println)
    sc.stop()
  }

  def groupByKey(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val sourceList = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class1", 90), Tuple2("class2", 60))

    val sources: RDD[(String, Int)] = sc.parallelize(sourceList)

    val groupedSources: RDD[(String, Iterable[Int])] = sources.groupByKey()

    groupedSources.foreach(source => {
      println(source._1)
      source._2.foreach(singleScore => println(singleScore))
      println("-----------------------------")
    })

    sc.stop()
  }


  def reduceByKey(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val sourceList = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class1", 90), Tuple2("class2", 60))

    val sources: RDD[(String, Int)] = sc.parallelize(sourceList)

    val totalScores: RDD[(String, Int)] = sources.reduceByKey(_ + _)

    totalScores.foreach(classSource => {
      println(classSource._1 + ":" + classSource._2)
    })

    sc.stop()
  }

  def sortByKey(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val sourceList = Array(Tuple2(80, "tom"), Tuple2(50, "tom"), Tuple2(100, "marry"), Tuple2(86, "jack"))

    val sources: RDD[(Int, String)] = sc.parallelize(sourceList, 1)

    val sortedSource: RDD[(Int, String)] = sources.sortByKey(false)

    sortedSource.foreach(classSource => {
      println(classSource._1 + ":" + classSource._2)
    })

    sc.stop()
  }


}