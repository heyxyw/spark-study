package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Create by zhouq on 2019/9/10
 */
public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建 hiveContext 这里接收的是SparkContext 作为参数 不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());

        // 第一个功能，使用 hiveConytext 的sql()/hql() 方法，可以执行Hive 中能够执行的 HiveQL 语句

        // 判断是否存在 student_infos 表，如果存在则删除
        hiveContext.sql("DROP TABLE IF EXISTS  student_infos");

        // 判断student_infos 表书否存在，不存在，则创建该表
        hiveContext.sql("CREATE TABLE IF NOT EXISTX  student_infos (name STRING,age INT)");

        // 将学生基本信息导入student_infos 表中。
        hiveContext.sql("LOAD DATA " +
                "LOCAL PATH '/loca/xxx/student_info.txt' " +
                "INTO TABLE student_infos");
        // 用同样的方式给 student_scores 导入数据
        hiveContext.sql("DROP TABLE IF EXISTS  student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores(name STRING,score INT)");
        hiveContext.sql("LOAD DATA  " +
                "LOCAL PATH '/local/xxx/student_scores.txt' " +
                "INTO TABLE student_scores");

        // 第二个功能，执行sql 还可以返回 DataFrame 用于查询。

        // 执行SQL 查询，关联两张表，查询成绩大于 80分 的学生。
        DataFrame goodStudentDF = hiveContext.sql("SELECT si.name,si.age,ss.score " +
                "FROM student_infosv si " +
                "JOIN student_scores ss ON si.name = ss.name " +
                "WHERE ss.score => 80");

        // 第三个功能，DataFrame 中的数据为Row ，就可以将 DataFrame 中的数据直接保存到表中。

        // 将得到的数据保存到 good_student_infos 表中
        hiveContext.sql("DROP TABLE IF EXISTS  good_student_infos");
        goodStudentDF.saveAsTable("good_student_infos");

        // 第四个功能，可以用table() 方法，针对Hive 表，直接创建DataFrame

        // 然后针对 good_student_infos 表直接创建 DataFrame
        Row[] goodStudentInfosRow = hiveContext.table("good_student_infos").collect();

        for (Row row : goodStudentInfosRow) {
            System.out.println(row);
        }

        sc.stop();
    }
}
