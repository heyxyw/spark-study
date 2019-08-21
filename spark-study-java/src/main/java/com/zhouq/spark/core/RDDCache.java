package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD  的持久化
 * Create by zhouq on 2019/8/18
 */
public class RDDCache {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDCache").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = sc.textFile("H:\\bigdatatest\\spark\\access.log").cache();

//        System.out.println(linesRDD);
//        JavaRDD<String> cache = linesRDD.cache();
//        System.out.println(cache);

        long startTime = System.currentTimeMillis();

        long count = linesRDD.count();

        long endTime = System.currentTimeMillis();

        System.out.println("行数：" + count + " 耗时：" + (endTime - startTime));

        startTime = System.currentTimeMillis();

        count = linesRDD.count();

        endTime = System.currentTimeMillis();

        System.out.println("行数：" + count + "耗时：" + (endTime - startTime));

    }
}
