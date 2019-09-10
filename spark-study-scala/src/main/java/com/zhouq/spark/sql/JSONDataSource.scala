package com.zhouq.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Create by zhouq on 2019/9/10
  *
  */
object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentSourceDF: DataFrame = sqlContext.read.json("H:\\bigdatatest\\spark\\sql\\students_score.json")

    // 注册
    studentSourceDF.registerTempTable("students_score")

    // 查询大于80的 学生
    val goodStudentScoreDF: DataFrame = sqlContext.sql("select name,score from students_score where score > 80")
    // 提取学生名称列表
    val goodStudentNames: Array[Any] = goodStudentScoreDF.rdd.map(row => row(0)).collect()

    // 处理学生信息数据
    val studentInfoJsons = Array("{\"name\":\"Leo\", \"age\":18}", "{\"name\":\"Marry\", \"age\":17}", "{\"name\":\"Jack\", \"age\":19}")
    // 转换为 RDD
    val studentInfoJSONSRDD: RDD[String] = sc.parallelize(studentInfoJsons, 1)

    // 转换为 DF
    val studentInfoJsonDF: DataFrame = sqlContext.read.json(studentInfoJSONSRDD)
    // 注册临时表
    studentInfoJsonDF.registerTempTable("student_info")

    // 查询分数大于80 分的学生的基本信息
    var sql = "select name,age from  student_info where name in ("

    for (i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if (i < goodStudentNames.length - 1) {
        sql += ","
      }
    }
    sql += ")"

    val goodStudentInfoDF: DataFrame = sqlContext.sql(sql)

    // 将分数大于80分的学生信息跟成绩信息进行 join

    val goodStudentsRDD: RDD[(String, (Long, Long))] = goodStudentScoreDF.rdd
      .map(
        row => (row.getAs[String]("name"), row.getAs[Long]("score"))
      )
      .join(
        goodStudentInfoDF.rdd
          .map(
            row => (row.getAs[String]("name"), row.getAs[Long]("age"))
          )
      )

    // 将RDD 转换为 DataFrame
    val goodStudentRowsRDD: RDD[Row] = goodStudentsRDD.map(info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)
    ))

    val goodStudentsDF: DataFrame = sqlContext.createDataFrame(goodStudentRowsRDD, structType)

    // 将DF 中的数据保存为 json 文件
    goodStudentsDF.write.format("json").save("H:\\bigdatatest\\spark\\sql\\out\\good-students-scala")

    sc.stop()

  }

}
