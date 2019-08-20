package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 取数字前三
 *
 * Create by zhouq on 2019/8/20
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> numbers = sc.textFile("G:\\stady\\scalatest\\number.txt");

        JavaPairRDD<Integer, String> pairs = numbers.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String numberStr) throws Exception {
                return new Tuple2<>(Integer.valueOf(numberStr), numberStr);
            }
        });

        JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);

        JavaRDD<Integer> sortedNumber = sortedPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2._1;
            }
        });

        List<Integer> top3NumberList = sortedNumber.take(3);

        for (Integer number: top3NumberList){
            System.out.println(number);
        }

        sc.stop();
    }
}
