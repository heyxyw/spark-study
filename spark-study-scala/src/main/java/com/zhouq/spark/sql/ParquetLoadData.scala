package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/9/9
  *
  */
object ParquetLoadData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParquetLoadData")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val usersDF: DataFrame = sqlContext.read.load("H:\\bigdatatest\\spark\\sql\\users.parquet")

    usersDF.registerTempTable("users")

    val userNamesDF: DataFrame = sqlContext.sql("select name from users")

    userNamesDF.rdd.map(row => ("name:" + row.getString(0))).collect().foreach(println)

    sc.stop()
  }

}
