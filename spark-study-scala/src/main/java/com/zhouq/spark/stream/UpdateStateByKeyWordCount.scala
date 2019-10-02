package com.zhouq.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by zhouq on 2019/10/2
  *
  */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("H:\\bigdatatest\\spark\\check_point_workcount_scala")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop5", 9999)

    val words: DStream[String] = lines.flatMap(line => line.split(" "))

    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    val wordcount: DStream[(String, Int)] = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }

      Option(newValue)
    })

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
