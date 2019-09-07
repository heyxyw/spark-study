package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * //todo
 *
 * @Author: zhouq
 * @Date: 2019/9/6
 */
public class DataFreamOpeate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFreamCreate").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://namenode1:8020/tmp/test/zhouq/student.txt");
        //显示100 条数据 ,不带参数是显示 全部
        df.show(100);
        //打印schema 信息
        df.printSchema();
        //查询某列所有的数据
        df.select("name").show();
        // 查询几列的数据，并对列进行计算
        df.select(df.col("name"),df.col("age").plus(1)).show();
        // 根据一列进行过滤
        df.filter(df.col("age").gt(23)).show();
        //根据某一行分组，然后聚合
        df.groupBy(df.col("age")).count().show();
        sc.stop();
    }
}
