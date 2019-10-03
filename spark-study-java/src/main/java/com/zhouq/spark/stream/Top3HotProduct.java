package com.zhouq.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 与Spark SQL 整合使用，top3 热门商品实时统计
 *
 * @Author: zhouq
 * @Date: 2019/10/3
 */
public class Top3HotProduct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 输入日志的格式
        // leo  iphone mobile_phone

        // 获取输入数据流

        JavaReceiverInputDStream<String> productClickLogsDStream = jssc.socketTextStream("localhost", 9999);

        // 然后是做一个映射，将每个种类的每个商品，映射成 （category_profuct,1） 的这种格式，
        // 从而可以在后面使用window 操作，对窗口中的这种格式的数据，进行reduceByKey 操作
        // 从而统计出来，一个窗口种的每个种类的每个商品的点击次数。

        JavaPairDStream<String, Integer> categoryProductPairsDStream =
                productClickLogsDStream.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String productClickLog) throws Exception {
                        String[] productClickLogSplited = productClickLog.split(" ");

                        return new Tuple2<>(productClickLogSplited[2] + "_" + productClickLogSplited[1], 1);
                    }
                });

        // 然后执行window 操作
        // 到这里，就可以做到每隔10秒，对最近60 秒的数据执行reduceByKey操作，
        // 计算出来这60秒内，每个种类的每个商品的点击次数
        JavaPairDStream<String, Integer> categoryProductCountsDStream =
                categoryProductPairsDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.seconds(60), Durations.seconds(10));

        // 然后对60秒内的每个种类的每个商品的点击次数执行 foreachRDD 在内部使用Spark SQL 执行 top3 热门商品的统计
        categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                // 将该RDD 转化成 JavaRDD<Row> 的格式
                JavaRDD<Row> categoryProductCountRowRDD = categoryProductCountsRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> categoryProductCount) throws Exception {
                        String category = categoryProductCount._1.split("_")[0];
                        String product = categoryProductCount._1.split("_")[1];
                        Integer count = categoryProductCount._2;
                        return RowFactory.create(category, product, count);
                    }
                });

                // 然后，执行DateFrame 转换

                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));

                StructType structType = DataTypes.createStructType(structFields);

                // 创建 sqlcontext
                HiveContext hiveContext = new HiveContext(categoryProductCountsRDD.context());
//                SQLContext sqlContext = new SQLContext(categoryProductCountsRDD.context());
                // 创建 DF
                DataFrame categoryProductCountDF = hiveContext.createDataFrame(categoryProductCountRowRDD, structType);
                //注册临时表
                categoryProductCountDF.registerTempTable("product_click_log");

                // 执行SQL 语句，针对临时表，统计出来每个种类下点击次数前三的热门商品
                DataFrame top3productDF = hiveContext.sql(
                        "select category,product,click_count " +
                                "from (" +
                                " select " +
                                " category," +
                                " product," +
                                " click_count," +
                                " row_number() over (partition by category order by click_count desc) rank " +
                                " from product_click_log" +
                                ") tmp " +
                                " where rank <=3" +
                                " ");
                top3productDF.show();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
