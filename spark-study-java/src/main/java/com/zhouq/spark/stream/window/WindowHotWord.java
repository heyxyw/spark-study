package com.zhouq.spark.stream.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * 热点搜索词滑动窗口案例
 * Create by zhouq on 2019/10/2
 */
public class WindowHotWord {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 这里的搜索日志的格式
        // leo hello
        // tom world
        JavaReceiverInputDStream<String> searchLogDStream = jssc.socketTextStream("localhost", 9999);

        // 将搜索日志给转换成只有一个搜索词即可
        JavaDStream<String> searchWordDStream = searchLogDStream.map(new Function<String, String>() {
            @Override
            public String call(String searchLog) throws Exception {
                return searchLog.split(" ")[1];
            }
        });

        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        // 针对（sraechWord,1） 的tuple 格式的DStream 执行 reduceByKeyAndWindow 滑动窗口操作

        // 第二个参数是窗口长度，这里是60秒。
        // 第三个参数是滑动间隔，是10秒
        // 也就是每个10秒中，将最近60秒的数据作为一个窗口，进行内部的RDD聚合，然后统一对一个RDD 进行后续计算。
        // 所以说，这里的意思是之前的searchWordPairDStream 为止，都不会进行计算的，而是只放到哪里。
        // 然后等待我们的滑动间隔时间到了以后，10 秒钟到了，会将之前60秒的RDD ，我们一个batch 间隔是5秒，
        // 这里60秒就是 12 个RDD给聚合起来，然后统一执行reduceByKey 操作。

        // 这里的 reduceByKeyAndWindow 是针对整个窗口执行计算的，不是针对某个 DStream 中的RDD
        JavaPairDStream<String, Integer> searchWordCountsDStream =
                searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.seconds(60), Durations.seconds(10));


        // 到这里为止，就已经可以做到，每隔10秒中出来之前 60 秒收集到的单词的统计计数。

        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    @Override
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {

                        // 执行搜索词和出现频率的反转
                        JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD.mapToPair(
                                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                                    @Override
                                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                                        return new Tuple2<>(tuple2._2, tuple2._1);
                                    }
                                });

                        // 降序排序
                        JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false);

                        // 然后再次执行反转，变成(searchWord, count)的这种格式
                        JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.mapToPair(
                                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                                    @Override
                                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                                        return new Tuple2<>(tuple2._2, tuple2._1);
                                    }
                                });

                        // 然后用take()，获取排名前3的热点搜索词
                        List<Tuple2<String, Integer>> hogSearchWordCounts = sortedSearchWordCountsRDD.take(3);

                        // 这里的输出，暂时只是打印一下。
                        for (Tuple2<String, Integer> hogSearchWordCount : hogSearchWordCounts) {
                            System.out.println(hogSearchWordCount._1 + ":" + hogSearchWordCount._2);
                        }

                        return searchWordCountsRDD;
                    }
                });

        //
        finalDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
