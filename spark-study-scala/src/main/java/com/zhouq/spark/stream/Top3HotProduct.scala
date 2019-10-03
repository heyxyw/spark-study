package com.zhouq.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * //todo
 *
 * @Author: zhouq
 * @Date: 2019/10/3
 */
object Top3HotProduct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct")

    val ssc = new StreamingContext(conf, Seconds(1))
    val productClickLogsDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop5", 9999)

    val categoryProductLogsPairsDFstream = productClickLogsDStream.
      map(productClickLog => {
        (productClickLog.split(" ")(2) + "_" + productClickLog.split(" ")(1), 1)
      })

    val categoryProductCountsDStream = categoryProductLogsPairsDFstream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )

    categoryProductCountsDStream.foreachRDD(categoryProductCountsRDD => {
      val categoryProductCountRowRDD = categoryProductCountsRDD.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)
      })
      // 创建scama 信息
      val structType = StructType(Array(
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("click_count", IntegerType, true)
      ))

      // 创建hiveContext
      val hiveContext = new HiveContext(categoryProductCountsRDD.context)
      // 创建 DF
      val crtegoryProductCountDF = hiveContext.createDataFrame(categoryProductCountRowRDD, structType)

      // 注册临时表
      crtegoryProductCountDF.registerTempTable("product_click_log")

      val top3productDF = hiveContext.sql("" +
        "select category,product,click_count "
        + "from ("
        + " select "
        + " category,"
        + " product,"
        + " click_count,"
        + " row_number() over (partition by category order by click_count desc) rank "
        + " from product_click_log"
        + ") tmp "
        + " where rank <=3"
        + " ")
      top3productDF.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
