package com.zhouq.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 基于HDFS 的实时wordcount
 * <p>
 * 注意：
 * Spark Streaming会监视指定的HDFS目录，并且处理出现在目录中的文件;
 * <p>
 * 所有放入HDFS目录中的文件，都必须有相同的格式；
 * <p>
 * 必须使用移动或者重命名的方式，将文件移入目录；
 * <p>
 * 一旦处理之后，文件的内容即使改变，也不会再处理了；
 * <p>
 * 基于HDFS文件的数据源是没有Receiver的，因此不会占用一个cpu core。
 * <p>
 * Create by zhouq on 2019/9/24
 */
public class HDFSWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 首先，使用JavaStreamingContext 的 textFileStream() 方法，针对HDFS 目录创建输入流
        JavaDStream<String> lines = jssc.textFileStream("hdfs://spark1:9000/xxx/input");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(","));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        wordCount.print();
        Thread.sleep(5000);


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
