package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Parquet 元数据合并案例：
  *
  *
  * Create by zhouq on 2019/9/9
  *
  */
object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ParquetMergeSchema").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 创建一个DataFrame 作为学生的基本信息，并写入一个 parquet 文件中
    val studentsWithNameAge: Seq[(String, Int)] = Array(("zhouq", 23), ("heyx", 18)).toSeq

    val studentsWithNameAgeDF: DataFrame = sc.parallelize(studentsWithNameAge, 2).toDF("name", "age")

    studentsWithNameAgeDF.save("H:\\bigdatatest\\spark\\sql\\out\\students", "parquet", SaveMode.Append)

    // 创建第二个 DataFrame 作为学生的成绩信息，并写入一个 parquet 文件中

    val studentWithNameGrade: Seq[(String, String)] = Array(("leo", "A"), ("tom", "B")).toSeq

    val studentWithNameGradeDF: DataFrame = sc.parallelize(studentWithNameGrade, 2).toDF("name", "grade")
    studentWithNameGradeDF.save("H:\\bigdatatest\\spark\\sql\\out\\students", "parquet", SaveMode.Append)

    // 用margeSchema 的方式，读取students 表中的数据，进行元数据合并

    val students: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("H:\\bigdatatest\\spark\\sql\\out\\students")

    students.printSchema()

    students.show()

    sc.stop()

  }

}
