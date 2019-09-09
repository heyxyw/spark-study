package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * SaveModel 示例
 * <p>
 * Create by zhouq on 2019/9/9
 */
public class SaveModelTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SaveModelTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read()
                .format("json")
                .load("H:\\bigdatatest\\spark\\sql\\people.json");


        // SaveMode.ErrorIfExists 存在抛错
//        usersDF.save("H:\\bigdatatest\\spark\\sql\\people.json", "json", SaveMode.ErrorIfExists);

        // SaveMode.Append 存在追加
        usersDF.save("H:\\bigdatatest\\spark\\sql\\out\\people3", "json", SaveMode.Append);

        // SaveMode.Overwrite 存在覆盖
//        usersDF.save("H:\\bigdatatest\\spark\\sql\\people.json", "json", SaveMode.Overwrite);

        // SaveMode.Ignore 什么都不做
//        usersDF.save("H:\\bigdatatest\\spark\\sql\\people.json", "json", SaveMode.Ignore);

        sc.stop();
    }
}
