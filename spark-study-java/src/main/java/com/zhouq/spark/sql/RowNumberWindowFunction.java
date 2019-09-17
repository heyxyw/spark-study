package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Create by zhouq on 2019/9/17
 * row_number() 开窗函数
 */
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RowNumberWindowFunction");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc.sc());

        HiveContext hiveContext = new HiveContext(sc);

        // 创建销售额表，sales表

        hiveContext.sql("DROP TABLE IF EXISTS sales");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
                + "product STRING,"
                + "category STRING,"
                + "revenue BIGINT )"
        );

        hiveContext.sql(" LOAD DATA " +
                "LOCAL INPATH '/usr/local/xxx/xxx.txt' " +
                "INTO TABLE sales");

        // 开始编写我们的统计逻辑，使用row_number() 开窗函数
        // 先说一下 row_number() 开窗函数的作用。
        // 其实就是给每个分组的数据，按照其排序的顺序，打上一个分组内的行号
        // 比如说，有一个分组 date=20181001 里面有三条数据，1122,1121,1123
        // 那么对这个分组的每一样使用 row_number() 开窗函数以后，这三行会依次获取一个从1开始递增的行号
        // 比如 1122 1,1121 2,1123 3

        DataFrame top3SalesDF = hiveContext.sql(""
                + "SELECT product,category,revenue "
                + "FROM ("
                + "SELECT "
                + "product,"
                + "category,"
                + "revenue,"
                + "ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue) rank"
                + "FROM sales "
                + ") tmp_sales"
                + ") WHERE rank <=3");

        hiveContext.sql("DROP TABLE IF EXISITS top3_sales");

        top3SalesDF.saveAsTable("top3_sales");

    }
}
