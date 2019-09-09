package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by zhouq on 2019/9/7
  *
  */
object DataFreamOpeate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataFreamOpeate").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.json("H:\\bigdatatest\\spark\\students.json")

    // 打印全部数据
    df.show()

    // 打印schema 信息
    df.printSchema()
    // 查询列的所有数据
    df.select(df.col("name")).show()
    df.select(col = "name").show()

    // 查询几列的数据，并对列进行计算，年龄加1
    df.select(df.col("name"),df.col("age").+(1)).show()
//    df.select(df.col("name"),df.col("age").gt(1)).show()
    // 根据某一列进行过滤，年龄 大于 18
    df.filter(df.col("age").>(18)).show()
//    df.filter(df.col("age").lt(18)).show()
    // 根据某一列分组，然后聚合
    df.groupBy(df.col("age")).count().show()

    sc.stop()
  }

}
