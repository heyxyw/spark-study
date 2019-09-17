package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 *
 * Create by zhouq on 2019/9/9
 *
 */
public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ManuallySpecifyOptions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read()
                .format("json")
                .load("H:\\bigdatatest\\spark\\sql\\people.json");

        usersDF.write()
                // 可选项 "parquet", "json"
                .format("json")
                .save("H:\\bigdatatest\\spark\\sql\\out\\people2");

        sc.stop();
    }
}
