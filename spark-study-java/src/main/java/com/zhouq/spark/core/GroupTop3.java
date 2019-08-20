package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Create by zhouq on 2019/8/20
 */
public class GroupTop3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("groupTop3").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("G:\\stady\\scalatest\\source.txt");

        JavaPairRDD<String, Integer> sourcePair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] lineSplited = line.split(" ");
                return new Tuple2<>(lineSplited[0], Integer.valueOf(lineSplited[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupPair = sourcePair.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Source = groupPair.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classSources) throws Exception {

                Integer[] top3 = new Integer[3];

                //class
                String className = classSources._1;
                Iterator<Integer> sources = classSources._2.iterator();

                while (sources.hasNext()) {
                    Integer source = sources.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = source;
                            break;
                        } else if (source > top3[i]) {
                            for (int j = 2; j > 1; j--) {
                                top3[j] = top3[j - 1];
                            }
                            top3[i] = source;
                            break;
                        }
                    }
                }

                return new Tuple2<>(className, Arrays.asList(top3));
            }
        });

        top3Source.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println( tuple2._1);

                Iterator<Integer> sourceInertor = tuple2._2.iterator();
                while (sourceInertor.hasNext()) {
                    Integer source = sourceInertor.next();
                    System.out.println(source);
                }

                System.out.println("========================");
            }
        });


        sc.stop();

    }
}
