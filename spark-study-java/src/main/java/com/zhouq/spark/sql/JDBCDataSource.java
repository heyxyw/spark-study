package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Create by zhouq on 2019/9/10
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);


        HashMap<String, String> options = new HashMap<>();

        options.put("url", "jdbc:mysql://192.168.0.101:3306/testdb");
        options.put("dbtable", "student_infos");

        DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.clear();

        options.put("url", "jdbc:mysql://192.168.0.101:3306/testdb");
        options.put("dbtable", "student_scores");
        DataFrame studentScoresDF = sqlContext.read().format("jdbc").options(options).load();

        // 将两个DF 转化为 JavaPairRDD 执行JOIN 操作。
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsPairRDD =
                studentInfosDF.javaRDD().mapToPair(
                        new PairFunction<Row, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Row row) throws Exception {
                                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
                            }
                        }).join(studentScoresDF.javaRDD().mapToPair(
                        new PairFunction<Row, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Row row) throws Exception {
                                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
                            }
                        }));

        JavaRDD<Row> studentRowsRDD = studentsPairRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
            }
        });

        // 过滤出学生成绩大于 80 分的
        JavaRDD<Row> filteredStudentRowsRDD = studentRowsRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;
            }
        });

        // 转换为DataFrame
        List<StructField> structFields = new ArrayList<>();

        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame studentsDF = sqlContext.createDataFrame(filteredStudentRowsRDD, structType);

        // 将DF 中的数据保存到Mysql中去。
        options.put("dbtable", "good_student_info");

//        studentsDF.write().format("jdbc").options(options).save();
        // 使用原生的jdbc 插入数据。

        sc.stop();

    }
}
