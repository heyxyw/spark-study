package com.zhouq.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Create by zhouq on 2019/9/8
  * 使用反射的方式将RDD 映射成 DataFrame
  *
  *
  * 如果要用 scala 开发 spark 程序，
  * 然后在其中使用基于反射的RDD 到DataFrame 的转换，就必须得用 object extends App 的方式
  * 不能用 def main 的方式来启动，不然会报 no typetag for 。。。 class 的错误
  */
object RDD2DataFrameReflection extends App {
  val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class Student(id: Int, name: String, age: Int)

  // 在Scala 中使用反射方式，进行RDD 到 DataFrame 的转换，需要手动导入一个 隐式转换，才可以使用它的 toDF() 方法
  import sqlContext.implicits._

  val studentsDF = sc.textFile("H:\\bigdatatest\\spark\\students.txt", 1)
    .map(line => line.split(","))
    .map(arr => Student(arr(0).toInt, arr(1).trim, arr(2).toInt))
    .toDF()

  studentsDF.registerTempTable("students")

  val teenagerDF: DataFrame = sqlContext.sql("select * from students where age <= 18")

  val teenagerRDD: RDD[Row] = teenagerDF.rdd

  teenagerRDD.map(row => Student(row.getInt(0), row.getString(1), row.getInt(2)))
    .collect()
    .foreach(println)

  // scala 中，row中的列的顺序是按照我们期望的顺序来排列的。
  // 也可以使用 row.getAs() 方法，获取指定的列，比在Java 中更丰富
  teenagerRDD.map(row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))
    .collect()
    .foreach(println)

  // 还有种方式 使用 row.getValuesMap，获取指定的列的数据
  teenagerRDD.map(row => {
    val map: Map[String, Any] = row.getValuesMap[Any](Array("id", "name", "age"))

    Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
  }).collect().foreach(println)

  sc.stop()

}
