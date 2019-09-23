package com.zhouq.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by zhouq on 2019/9/23
  *
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")

    val ssc = new StreamingContext(conf, Seconds(2))

    // 读取socket 数据

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop5", 9999)

    val words: DStream[String] = lines.flatMap(line => line.split(" "))

    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    Thread.sleep(5000)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
