package com.zhouq.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于Receiver 的方式接受Kafka数据进行实时 Wordcount
 * <p>
 * Create by zhouq on 2019/9/25
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("kafkaWordcount");
        JavaStreamingContext jss = new JavaStreamingContext(conf, Durations.seconds(5));


        Map<String, Integer> topicThreadMap = new HashMap();
        topicThreadMap.put("wordcount", 1);

        // 使用KafkaUtils.createStream() 方法，创建针对Kafka 的输入流
        // 得到的是
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jss,
                "",
                "",
                topicThreadMap
        );

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> tuple) {


                return Arrays.asList(tuple._2.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<>(word, 1);
            }
        });


        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1 + v2;
            }
        });

        wordCount.print();

        jss.start();
        jss.awaitTermination();
        jss.close();
    }
}
