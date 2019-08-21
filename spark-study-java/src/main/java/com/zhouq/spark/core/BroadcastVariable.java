package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Create by zhouq on 2019/8/18
 * 广播变量 ,Driver 把变量拷贝一份到 每一个Executor 中，所有的Task 在自己的 Executor 中获取。
 * 不使用 broadcast 广播，则是 Driver 会把变量都拷贝到 每一个Task 中。
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int factor = 3;

        // 在Java 中创建共享变量，使用 SparkContext 的 broadcast 方法
        // 获取返回值结果就是 Broadcast<T>
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        JavaRDD<Integer> numbers = sc.parallelize(numberList, 1);

        JavaRDD<Integer> multipleNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                // 使用 Broadcast 的 value() 方法获取共享变量。
                Integer factor = factorBroadcast.value();
                return v1 * factor;
            }
        });

        multipleNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer number) throws Exception {
                System.out.println(number);
            }
        });

        sc.stop();
    }
}
