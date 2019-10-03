package com.zhouq.spark.stream.window

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by zhouq on 2019/10/2
  *
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord")

    val ssc = new StreamingContext(conf, Seconds(5))

    val searchLogsDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop5", 9999)

    val searchWordsDStream: DStream[String] = searchLogsDStream.map(_.split(" ")(1))

    val searchWordPairsDStream: DStream[(String, Int)] = searchWordsDStream.map(searchWord => (searchWord, 1))

    val searchWordCountsDSteram: DStream[(String, Int)] = searchWordPairsDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60),
      Seconds(10))

    val finalDStream: DStream[(String, Int)] = searchWordCountsDSteram.transform(searchWordCountsRDD => {

      val countsearchWordsRDD: RDD[(Int, String)] = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))

      val sortedCountSearchWordsRDD: RDD[(Int, String)] = countsearchWordsRDD.sortByKey(false)

      val sortedSearchWordCountsRDD: RDD[(Int, String)] = sortedCountSearchWordsRDD.map(tuple => (tuple._1, tuple._2))

      val top3SearchWordCounts: Array[(Int, String)] = sortedSearchWordCountsRDD.take(3)

      for (tuple <- top3SearchWordCounts) {
        print(tuple)
      }

      searchWordCountsRDD
    })

    finalDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
