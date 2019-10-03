package com.zhouq.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * Create by zhouq on 2019/9/24
  *
  */
object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount")

    val ssc = new StreamingContext(conf, Seconds(2))

    val wordCount: DStream[(String, Int)] = ssc.textFileStream("")
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    Thread.sleep(5000)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
