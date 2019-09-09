package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式动态指定元数据，将RDD 转化为 DataFrame
 * Create by zhouq on 2019/9/8
 */
public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameProgrammatically").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //第一步，创建一个普通的 RDD，但是，必须将其转换为 RDD<Row> 的这种格式
        JavaRDD<String> lines = sc.textFile("H:\\bigdatatest\\spark\\students.txt");

        JavaRDD<Row> studentsRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                // 注意，这里在创建row 的时候，如果这里的类型跟 StructField 下面定义的类型不一致，会报错。
                // 比如 String 不能转换成 Integer，报错是在 执行SQL 时候。
                return RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));
            }
        });

        //第二步，动态构造元数据
        // 比如说，id，name 等filed 的名称和类型，可能都是在程序运行过程中，动态从mysql 或者配置文件里面
        // 加载出来的，不是固定的。
        // 所以就特别适合这种编程方式来构造元数据
        List<StructField> fileds = new ArrayList<StructField>();

        fileds.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fileds.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fileds.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(fileds);

        //第三步，使用动态构造的元数据，将RDD 转换为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentsRDD, structType);

        // 然后就可以进行后面的逻辑了。注册临时表，执行sql 查询等等。。。
        studentDF.registerTempTable("students");

        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        JavaRDD<Row> teenagerRDD = teenagerDF.toJavaRDD();

        List<Row> rows = teenagerRDD.collect();

        for (Row row : rows) {
            System.out.println(row);
        }

        sc.stop();
    }
}
