package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 *
 * @Author: zhouq
 * @Date: 2019-08-20
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/zhouqiao/dev/test/sparktestdata/sort.txt");

        //把数据包装成 （SecondarysortKey，line） 的形式
        JavaPairRDD<SecondarysortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarysortKey, String>() {
            @Override
            public Tuple2<SecondarysortKey, String> call(String line) throws Exception {
                SecondarysortKey key = new SecondarysortKey(Integer.valueOf(line.split(" ")[0]), Integer.valueOf(line.split(" ")[1]));
                return new Tuple2<>(key, line);
            }
        });

        //排序
        JavaPairRDD<SecondarysortKey, String> sortedPairs = pairs.sortByKey();

        //去掉key
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarysortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarysortKey, String> tuple2) throws Exception {
                return tuple2._2;
            }
        });

        //遍历
        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });

        sc.stop();

    }
}
