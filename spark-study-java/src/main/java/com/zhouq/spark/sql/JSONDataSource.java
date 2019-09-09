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
import java.util.List;

/**
 * JsonData 数据源综合示例
 * <p>
 * Create by zhouq on 2019/9/9
 */
public class JSONDataSource {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JSONDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);


        DataFrame students_scoreDF = sqlContext.read().json("H:\\bigdatatest\\spark\\sql\\students_score.json");

        students_scoreDF.registerTempTable("students_score");

        // 针对学生基本信息DataFrame，注册临时表，然后查询分数大于80分的学生的基本信息
        DataFrame goodStudentScoresDF = sqlContext.sql("select name,score from students_score where score > 80");

        List<String> goodStudentNames = goodStudentScoresDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        //
        List<String> studentInfoJSONs = new ArrayList<String>();

        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");

        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);

        DataFrame studentsInfoDF = sqlContext.read().json(studentInfoJSONsRDD);

        studentsInfoDF.registerTempTable("students_Info");

        String sql = "select name,age from students_Info where name in (";

        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";

        DataFrame goodStudentsInfoDF = sqlContext.sql(sql);

        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation

        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                // name,age
                goodStudentsInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
                    }
                }).join(
                        // name,score
                        goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Row row) throws Exception {
                                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
                            }
                        }));


        // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {

                return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
            }
        });

        //创建一份元数据信息

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);
        // 将好学生的全部信息保存到一个json文件中去
        goodStudentsDF.write().format("json").save("H:\\bigdatatest\\spark\\sql\\out\\good-students");

        sc.stop();
    }
}
