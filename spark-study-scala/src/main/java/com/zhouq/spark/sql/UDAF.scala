package com.zhouq.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Create by zhouq on 2019/9/17
  * 使用自定义函数计算同名的个数
  *
  */
object UDAF {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("UDAF")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("zhouq", "heyxw", "lzla", "heyxw", "zhouq")
    val namesRDD: RDD[String] = sc.parallelize(names, 5)
    val namesRowRDD: RDD[Row] = namesRDD.map(name => Row(name))
    // 定义scama
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF: DataFrame = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册一张表

    namesDF.registerTempTable("names")

    sqlContext.udf.register("strCount", new UDAF_StringCount)

    // 使用自定义函数

    sqlContext.sql("select name,strCount(name) from names group by name").collect().foreach(println)

    sc.stop()
  }

}
