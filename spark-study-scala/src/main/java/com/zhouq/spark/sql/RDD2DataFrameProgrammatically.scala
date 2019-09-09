package com.zhouq.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 以编程方式动态指定元数据，将RDD 转化为 DataFrame
  *
  * Create by zhouq on 2019/9/8
  *
  */
object RDD2DataFrameProgrammatically extends App {

  private val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrameProgrammatically").setMaster("local")

  private val sc = new SparkContext(conf)

  private val sqlContex = new SQLContext(sc)

  // 第一步，狗仔元素为 RDD 的普通RDD
  private val studentRDD: RDD[Row] = sc.textFile("H:\\bigdatatest\\spark\\students.txt", 1)
    .map(line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt))

  // 第二步，构造元数据信息
  private val structType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  // 第三步，进行RDD 到DataFrame 的转换

  private val studentDF: DataFrame = sqlContex.createDataFrame(studentRDD, structType)

  // 继续正常业务逻辑

  studentDF.registerTempTable("students")

  private val teenagerDF: DataFrame = sqlContex.sql("select * from students where age <= 18")

  teenagerDF.rdd.collect().foreach(println)

  sc.stop()
}
