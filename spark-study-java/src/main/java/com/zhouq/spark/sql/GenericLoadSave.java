package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * load  and save
 * <p>
 * 对于Spark SQL的DataFrame来说，无论是从什么数据源创建出来的DataFrame，都有一些共同的load和save操作。
 * load操作主要用于加载数据，创建出DataFrame；save操作，主要用于将DataFrame中的数据保存到文件中。
 * <p>
 * Create by zhouq on 2019/9/9
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GenericLoadSave");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read().load("H:\\bigdatatest\\spark\\sql\\users.parquet");

//        usersDF.printSchema();
//        usersDF.show();

        usersDF.select("name", "favorite_color")
                .write()
                .save("H:\\bigdatatest\\spark\\sql\\out\\users_favorite_color.parquet");

        sc.stop();
    }
}
