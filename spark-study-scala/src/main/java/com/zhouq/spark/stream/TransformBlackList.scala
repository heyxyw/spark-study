package com.zhouq.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by zhouq on 2019/10/2
  *
  */
object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[5]")
      .setAppName("TransformBlackList")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blackList = Array(Tuple2("tom", true))

    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blackList, 5)

    val adsClickLogDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop5", 9999)

    val userAdsClickLogDStream: DStream[(String, String)] = adsClickLogDStream.map(log => (log.split(" ")(1), log))

    val validAdsClickLogDStream: DStream[String] = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
      val joined: RDD[(String, (String, Option[Boolean]))] =
        userAdsClickLogRDD.leftOuterJoin(blackListRDD)

      val filtered: RDD[(String, (String, Option[Boolean]))] = joined.filter(tuple => {
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })

      val userAdsClickLog: RDD[String] = filtered.map(_._2._1)
      userAdsClickLog
    })

    validAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
