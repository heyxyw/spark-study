package com.zhouq.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Create by zhouq on 2019/9/21
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf对象
        // 但是这里有一点不同，我们是要给它设置一个Master属性，但是我们测试的时候使用local模式
        // local后面必须跟一个方括号，里面填写一个数字，数字代表了，我们用几个线程来执行我们的
        // Spark Streaming程序
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");

        // 创建JavaStreamingContext对象
        // 该对象，就类似于Spark Core中的JavaSparkContext，就类似于Spark SQL中的SQLContext
        // 该对象除了接收SparkConf对象对象之外
        // 还必须接收一个batch interval参数，就是说，每收集多长时间的数据，划分为一个batch，进行处理
        // 这里设置一秒

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 首先，创建输入DStream，代表了一个从数据源（比如kafka、socket）来的持续不断的实时数据流
        // 调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为Socket网络端口的
        // 数据流，JavaReceiverInputStream，代表了一个输入的DStream

        // socketTextStream()方法接收两个基本参数，第一个是监听哪个主机上的端口，第二个是监听哪个端口
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop5", 9999);

        // 到此为止，我们可以理解为 JavaReceiverInputStream 中的每隔一秒，就会有一个RDD，其中封装了这一秒发送过来的数据
        // RDD  的元素类型为String ，即一行一行的文本。
        // 所以，这里 JavaReceiverInputDStream的泛型类型为String，其实就代表了底层的RDD 的泛型类型

        // 开始对接收到的数据执行计算，使用Spark Core 提供的算子。执行应用在DStream 中即可。
        // 在底层，实际上会对DStream 中的一个一个RDD，执行我们应用在DStream 上的算子。
        // 产生的新RDD，会作为新DStream 中的RDD。
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        // 到这里，每一行的文本会拆分成为多个单词。words DStream 中的RDD的元素类型即为一个一个的单词。

        // 执行flatMap reduceByKey 操作

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });


        // 这里，正好说明一下，其实大家可以看到，用Spark Streaming开发程序，和Spark Core很相像
        // 唯一不同的是Spark Core中的JavaRDD、JavaPairRDD，都变成了JavaDStream、JavaPairDStream

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        Thread.sleep(5000);
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
