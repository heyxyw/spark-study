package com.zhouq.spark.stream.window;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于 updateStateByKey 算子实现缓存机制的实时Wordcount 程序
 * Create by zhouq on 2019/10/2
 */
public class PersistWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PersistWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("H:\\bigdatatest\\spark\\check_point_workcount");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop5", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {

                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(
                // Optional 可以理解为scala 的样例类，它代表一个值可以存在也可以不存在
                new Function2<List<Integer>,
                        Optional<Integer>, Optional<Integer>>() {

                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        // 首先定义一个全局的单词计数
                        Integer newValue = 0;

                        // 判断 state 是否存在，如果不存在，说明是一个 key 第一次出现
                        // 如果存在，说明这个key 之前已经统计过全局的次数了
                        if (state.isPresent()) {
                            newValue = state.get();
                        }

                        // 然后，将本次出现的值，都累计到newValue 上去，就是一个 key 目前的全局统计次数
                        for (Integer value : values) {
                            newValue += value;
                        }

                        return Optional.of(newValue);
                    }
                });


        // 每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便于后续的J2EE应用程序
        // 进行显示
        wordcounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wordCounts) throws Exception {

                wordCounts.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        //为每一个 partition 获取一个连接
                        Connection conn = ConnectionPool.getConnection();

                        //遍历partition 中的数据，使用同一个连接，插入数据库中

                        Tuple2<String, Integer> wordCount = null;
                        while (wordCounts.hasNext()) {
                            wordCount = wordCounts.next();

                            String sql = "insert into wordcount(word,count) "
                                    + "values('" + wordCount._1 + "'," + wordCount._2 + ")";

                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }

                        ConnectionPool.returnConnectiuon(conn);
                    }
                });

                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
