package com.zhouq.spark.sql

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * //todo
 *
 * @Author: zhouq
 * @Date: 2019/9/17
 */
object DailySale {
  def main(args: Array[String]): Unit = {
    val conf = new
        SparkConf().setAppName("DailySale").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 说明一下，业务的特点
    // 实际上，我们可以做一个，单独统计网站登陆用户的销售额统计

    // 有些时候，会出现日志的上报的错误和一次，比如日志里面丢了用户的信息，那么这种，我们就一律不统计了

    // 模拟数据
    val userSaleLog = Array(
      "2018-10-01,49.98,1122",
      "2018-10-01,99.32,",
      "2018-10-02,15.60,1444",
      "2018-10-02,32.89,1344",
      "2018-10-03,49.01,1555",
      "2018-10-03,94.03,1555",
      "2018-10-04,65.43,1222",
      "2018-10-04,64.67"
    )
    val userSaleLogRDD = sc.parallelize(userSaleLog,5)

    // 进行有效销售日志的过滤
    val filteredUserSaleLogRDD = userSaleLogRDD.filter(_.split(",").length == 3)

    // 对过滤完成的日志进行转换处理
    val userSaleLogRowRDD = filteredUserSaleLogRDD.map( log => Row(log.split(",")(0),log.split(",")(1).toDouble))

    //准备scama 信息
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)
    ))
    //构建 DataFrame
    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD,structType)

    //开始进行销售额进行每日统计
    userSaleLogDF.groupBy("date")
      .agg('date,sum('sale_amount))
      .map( row =>(Row(row(1),row(2))))
      .collect()
      .foreach(println)

    sc.stop()

  }

}
