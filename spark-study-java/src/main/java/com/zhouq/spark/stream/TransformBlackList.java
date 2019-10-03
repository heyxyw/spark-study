package com.zhouq.spark.stream;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 真实案例：实时广告日志黑名单过滤
 * <p>
 * Create by zhouq on 2019/10/2
 */
public class TransformBlackList {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlackList");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 用户对我们的网站上的广告可以进行点击
        // 点击之后，是不是要进行实时计费，点一下，算一次钱
        // 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
        // 只要是黑名单中的用户点击的广告，我们就给过滤掉

        // 首先模拟一份黑名单RDD
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));

//        final JavaPairRDD<String, Boolean> blockListRDD = jssc.sc().parallelizePairs(blacklist);
        final JavaPairRDD<String, Boolean> blockListRDD = jssc.sparkContext().<String, Boolean>parallelizePairs(blacklist);

        // 模拟点击日志 date username
        JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("hadoop5", 9999);

        // 首先对日志进行一下转化，变成（username,date username）,方便后面对每个batch RDD 与定义好的黑名单RDD 进行join 操作。

        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {

                return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
            }
        });

        // 然后就可以执行transform操作了，对每个batch 的RDD 与黑名单RDD  进行join、filter、map 操作。
        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {

                // 这里为什么用左外连接？
                // 因为，并不是每个用户都存在于黑名单中的
                // 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到
                // 就给丢弃掉了
                // 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到
                // 也还是会被保存下来的
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
                        userAdsClickLogRDD.<Boolean>leftOuterJoin(blockListRDD);

                // 执行 filter 算子，过滤黑名单中 为true 的数据
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {

                        // 这里的tuple，就是每个用户，对应的访问日志，和在黑名单中的状态

                        if (tuple._2._2().isPresent() && tuple._2._2().get()) {
                            return false;
                        }
                        return true;
                    }
                });

                // 此时，filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
                // 进行map操作，转换成我们想要的格式

                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(
                        new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {

                            @Override
                            public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                                return tuple._2._1;
                            }
                        });

                return validAdsClickLogRDD;
            }
        });

        // 打印有效的广告点击日志
        // 其实在真实企业场景中，这里后面就可以走写入kafka、ActiveMQ等这种中间件消息队列
        // 然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
        validAdsClickLogDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
