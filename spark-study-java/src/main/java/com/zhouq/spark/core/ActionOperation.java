package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
