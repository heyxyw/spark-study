package com.zhouq.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * 根据每天的访问日志和购买日志，统计uv和 销售额
 *
 * @Author: zhouq
 * @Date: 2019/9/16
 */
object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailyUV").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //要使用SparkSQL 的内置函数，必须倒入隐式转换
    import sqlContext.implicits._

    //构造用户访问日志数据，并创建DataFrame
    val userAccressLog = Array(
      "2018-10-01,1121",
      "2018-10-01,1121",
      "2018-10-01,1122",
      "2018-10-01,1123",
      "2018-10-01,1123",
      "2018-10-01,1124",
      "2018-10-02,1122",
      "2018-10-02,1122",
      "2018-10-02,1122",
      "2018-10-02,1123",
      "2018-10-02,1123",
      "2018-10-02,1124"
    )

    val userAccressLogRDD = sc.parallelize(userAccressLog,5)

    // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD 转换为元素为Row 的RDD
    val userAccressLogRowRDD = userAccressLogRDD.map(log =>Row(log.split(",")(0),log.split(",")(1).toInt))

    // 构造DataFrame 的元数据
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userid", IntegerType, true)
    ))

    //使用SQLContext 创建DataFrame
    val userAccressLogRowDF = sqlContext.createDataFrame(userAccressLogRowRDD,structType)

    // 这里使用内置函数，countDistinct
    // uv，是指对用户进行访问去重以后的访问总数
    // 聚合函数的用法：
    // 首先对DataFrame 调用groupBy 进行分组
    // 然后，调用agg 方法，第一个参数必须传入之前在groupBy 方法中出现的字段
    // 第二个参数，传入 countDistinct、sum、first 等，spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    userAccressLogRowDF.groupBy("date")
      .agg('date,countDistinct('userid))
      .map(row => Row(row(1),row(2)))
      .collect()
      .foreach(println)



    sc.stop()







  }

}
