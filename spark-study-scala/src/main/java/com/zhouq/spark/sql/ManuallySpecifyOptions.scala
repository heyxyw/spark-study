package com.zhouq.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Create by zhouq on 2019/9/9
  *
  */
object ManuallySpecifyOptions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val usersDF: DataFrame = sqlContext.read
      .format("json")
      .load("H:\\bigdatatest\\spark\\sql\\people.json")

    usersDF.select("name").write
      .format("parquet")
      .save("H:\\bigdatatest\\spark\\sql\\out\\people_name_scala")

    sc.stop()
  }

}
