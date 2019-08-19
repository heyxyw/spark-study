package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Create by zhouq on 2019/8/14
 */
public class ActionOperation {
    public static void main(String[] args) {

        countByKey();
    }


    public static void reduce(){
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 有一个集合，里面1到10 ，十个数字，现在要对10个数字进行累计求和。
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        /**
         * 使用 reduce 操作对集合中的数据进行累加
         * reduce 操作的原理：
         *      首先将第一个和第二个元素，传入call() 方法，进行计算，会得到一个结果，比如 1 + 2 = 3
         *      接着将该结果与下一个元素传入call() 方法，进行计算 比如 3+3 = 6
         *      以此类推
         *  所以reduce 操作的本质就是聚合，将多个元素聚合成一个元素
         */
        Integer sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.close();
    }

    public static void collect(){
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //
        JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

        //使用map 函数将集合内的元素乘以2
        JavaRDD<Integer> doubleNumbersRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        /**
         * 一般不建议使用 collect 操作，因为他会把各Executor 节点的数据都收集到 Driver 上来。
         * 如果一旦数量太大，可能会出现OOM，也会占用大量的网络传输
         *
         * 因此，通常建议使用  foreach action 操作，来对最终的 rdd 元素进行操作。
         */

        List<Integer> doubleNumberList = doubleNumbersRDD.collect();

        for (Integer num : doubleNumberList) {
            System.out.println(num);
        }

        sc.close();
    }

    public static void  count(){
        SparkConf conf = new SparkConf().setAppName("count").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);

        long count = numberListRDD.count();
        System.out.println(count);
        sc.stop();
    }

    public static void take(){
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(2, 23, 4, 5, 6, 7, 8, 9, 0);

        JavaRDD<Integer> numberListRDD = sc.parallelize(numberList);

        //take 操作，也是与collect 类似，也是从远程获取数据。
        List<Integer> top2Number = numberListRDD.take(2);

        for (Integer integer : top2Number) {
            System.out.println(integer);
        }

        sc.stop();
    }

    private static void saveAsTextFile() {

    }

    private static void countByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> sourceList = Arrays.asList(
                new Tuple2<>("class1", "lisi"),
                new Tuple2<>("class2", "wangwu"),
                new Tuple2<>("class1", "mazi"),
                new Tuple2<>("class2", "cuihua"),
                new Tuple2<>("class3", "liuer")
        );

        //并行化

        JavaPairRDD<String, String> studentsRDD = sc.parallelizePairs(sourceList);

        // 对RDD 应用 countByKey ，统计每个班级的学生人数。也就是统计每个key对应的元素个数
        // 也就是 countByKey 的作用
        // countByKey 返回值的类型，直接就是 Map<String,Object>
        Map<String, Object> studentCounts = studentsRDD.countByKey();

        for (Map.Entry<String, Object> studentCount : studentCounts.entrySet()) {
            System.out.println(studentCount.getKey() + ":" + studentCount.getValue());
        }

        //关闭 JavaSparkContext
        sc.stop();

    }

}
