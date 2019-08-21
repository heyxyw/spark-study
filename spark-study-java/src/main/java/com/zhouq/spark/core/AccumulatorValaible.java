package com.zhouq.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加变量
 *
 * @Author: zhouq
 * @Date: 2019-08-19
 */
public class AccumulatorValaible {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("accumulator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建 Accumulator 变量，需要通过 SparkContext.accumulator(0) 来创建
        Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v1) throws Exception {
                //然后在函数内部，就可以对Accumulator 变量，调用其add() 方法来累加值
                sum.add(v1);
            }
        });

        //在 Driver 中可以通过 Accumulator.value() 来获取它的值。
        System.out.println(sum.value());
        sc.stop();

    }
}
