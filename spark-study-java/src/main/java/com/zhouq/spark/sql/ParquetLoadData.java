package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Parquet 数据源加载之编程方式加载数据
 * Create by zhouq on 2019/9/9
 */
public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        // 读取一个 parquet 文件，创建一个DataFrame
        DataFrame usersDF = sqlContext.read().load("H:\\bigdatatest\\spark\\sql\\users.parquet");

        // 将DataFrame 注册为一个临时表，然后使用SQL 查询需要的数据
        usersDF.registerTempTable("users");
        DataFrame nameFromUsersDF = sqlContext.sql("select name from users");

        // 对查询出来的DataFrame 进行 transformation 操作，处理数据，然后打印出来
        List<String> userNames = nameFromUsersDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "name:" + row.getString(0);
            }
        }).collect();

        for (String userName : userNames) {
            System.out.println(userName);
        }
        sc.stop();
    }
}
