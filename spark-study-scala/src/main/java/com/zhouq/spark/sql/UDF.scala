package com.zhouq.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/9/17
  * 自定义函数
  *
  */
object UDF {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("UDF")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("zhouq", "heyxw", "lzla")
    val namesRDD: RDD[String] = sc.parallelize(names, 5)
    val namesRowRDD: RDD[Row] = namesRDD.map(name => Row(name))
    // 定义scama
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF: DataFrame = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册一张表

    namesDF.registerTempTable("names")

    sqlContext.udf.register("strLen", (str: String) => str.length())

    // 使用自定义函数

    sqlContext.sql("select name,strLen(name) from names").collect().foreach(println)

    sc.stop()
  }

}
