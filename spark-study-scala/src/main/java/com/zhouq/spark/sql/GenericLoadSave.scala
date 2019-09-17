package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/9/9
  *
  */
object GenericLoadSave {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GenericLoadSave").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val usersDF: DataFrame = sqlContext.read.load("H:\\bigdatatest\\spark\\sql\\users.parquet")

    usersDF.write.save("H:\\bigdatatest\\spark\\sql\\out\\users_favorite_color_scala")

    sc.stop()
  }

}
