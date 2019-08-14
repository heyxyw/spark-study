package com.zhouq.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Create by zhouq on 2019/7/24
 * transformation 操作开发实战
 * 1.map :将集合中的每个元素乘以 2
 * 2.fliter: 过滤出集合中的偶数
 * 3.flatMap: 将行拆分成单词
 * 4.groupByKey: 将每个班级的成绩进行分组
 * 5.reduceByKey: 统计每个班级的总分
 * 6.sortByKey: 将学生分数进行排序
 * 7.join : 打印每个学生的成绩
 * 8.cogroup: 打印每个学生的成绩
 */
public class TransformatiuonOperation {
    public static void main(String[] args) {
//        map();
//        filter();
//        flatMap();

//        groupByKey();

//        reduceByKey();
//        sortByKey();
        join();
    }


    /**
     * map 算子案例:将集合中的每个元素都乘以 2
     */
    public static void map() {
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local");

        //创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        //并行化集合,创建创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //使用 map 算子,将集合中的每个元素都乘以 2
        /**
         * map 算子,是对任何类型的RDD 都可以调用,
         * 在Java 中,Map 算子接收的参数是Function 对象
         * 创建的Function 对象,一定会让你设置第二个泛型参数,这个泛型参数,就是你返回新元素的类型
         * 在 call 方法内部,就可以对 原始 RDD 中的每一个元素进行各种处理和计算,并返回一个新的元素
         * 所有新的元素就会组成一个新的RDD
         */
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            // 传入call 方法的数据就是 1,2,3,4,5
            // 返回新元素的值就是 2,4,6,8,10
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        //打印新的RDD
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v1) throws Exception {
                System.out.println(v1);
            }
        });

        //关闭 JavaSparkContext
        sc.close();
    }

    /**
     * filter 算子操作,打印出偶数
     */
    public static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);


        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        // 对初始 RDD 执行 filter 算子,过滤出其中的偶数
        // filter 算子,传入的也是Function ,其他的使用注意点,实际上也是和map 一样的
        // 但是唯一不同的就是 call () 方法的返回值是 Boolean
        // 每一个初始RDD 中的元素,都会传入 call() 方法,此时你可以执行各种自定义的计算逻辑
        // 来判断这个元素是不是你需要的
        // 如果你想在新的 RDD 中保留这个元素,那么就返回 true ,否则不想保留这个元素 就返回flase
        JavaRDD<Integer> evenNumbers = numbersRDD.filter(new Function<Integer, Boolean>() {
            // 在这里,从 1到 10 都会传入进来
            //但是根据我们的逻辑,只有2,4,6,8,10 这几个偶数,会返回 true
            // 所以,只有偶数会保留下来,放在新的RDD 中
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        //打印元素
        evenNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.stop();
    }

    /**
     * flatMap 案例,将文本拆分成多个单词
     */
    public static void flatMap() {

        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lines = Arrays.asList(" i lova you", "me me da ");

        JavaRDD<String> linesRDD = sc.parallelize(lines);

        /**
         * 对 RDD 执行 flatMap 算子,将每一行文本拆分为多个单词
         * flatMap 算子,在Java中接收的参数是 FlatMapFunction
         * 我们需要自己定义 FlatMapFunction 的第二个参数泛型类型,即代表了返回的新元素的类型
         * call() 方法,返回的类型不是 U,而是 Iterable<U> ,这里的 U 也是与第二个泛型类型相同
         * flatMap 其实就是,接收原始RDD 中的每个元素,并进行各种逻辑的计算和处理,返回返回多个元素
         *
         * 多个元素封装在 Iterable 集合中,可以使用 ArrayList 等集合进行转换
         *
         * 新的 RDD 中,即封装了所有的新元素;也就是新的RDD 的大小一定是大于 原始RDD 的大小
         */
        JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            // 在这里 比如传入的是 i love you
            // call  返回的则是 Iterable<String>(i,lova,you)
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //打印一下
        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //关闭
        sc.stop();
    }

    /**
     * groupByKey: 将每个班级的成绩进行分组
     */
    public static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> sourceList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 70),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 60)
        );

        //并行化

        JavaPairRDD<String, Integer> sources = sc.parallelizePairs(sourceList);

        // 针对 sources RDD 执行 groupByKey 算子,对每个班级的成绩进行分组
        // groupByKey 返回的 还是JavaPairRDD
        // 但是,JavaPairRDD 的第一个泛型类型不变,第二个泛型类型变成了 Iterable 这种集合类型
        // 也就是说,按照 key 进行分组,那么每个 key 都可能会有多个 value ,此时多个value 聚合成了一个 Iterable
        // 那么接下来,我们是不是就可以通过 groupedSources 这种 JavaPairRDD ,很方便的处理某个分组内的数据.

        JavaPairRDD<String, Iterable<Integer>> groupedScores = sources.groupByKey();

        //打印 groupedScores
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {

                System.out.println("score=" + tuple._1);
                Iterator<Integer> iterator = tuple._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("-------------------------------");
            }
        });

        //关闭 JavaSparkContext
        sc.stop();

    }

    /**
     * reduceByKey: 将每个班级的总成绩
     */
    public static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> sourceList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 70),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 60)
        );

        //并行化

        JavaPairRDD<String, Integer> sources = sc.parallelizePairs(sourceList);

        // 针对 sources RDD ,执行 reduceByKey  算子。
        // reduceByKey 接收的参数是 Function2 类型。他有三个泛型参数，实际上代表了三个值
        // 第一个泛型参数和第二个泛型参数，代表了原始RDD 中原始的value 类型。
        // 因此对每个 key 进行reduce 都会依次将第一个,第二个value 传入,将值和第三个 value 传入.
        // 因此,此处会定义两个泛型,代表call 方法的两个传入参数类型.
        // 第三个泛型类型代表每次reduce 操作返回的值类型,默认也是与原始RDD 的value 类型相同的.
        // reduceByKey 算法但会的RDD,还是 JavaPairRDD<key,value>

        // 对每个key 都会将其 value 依次传入 call 方法.
        //从而聚合出每个key 对应的value
        // 然后,将每个keu 对应的 value 组合成一个Tuple2 作为新RDD 的元素

        JavaPairRDD<String, Integer> totalSources = sources.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        totalSources.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ":" + tuple2._2);
            }
        });

        //关闭 JavaSparkContext
        sc.stop();

    }


    /**
     * sortByKey: 按照学生的分数进行排序
     */
    public static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> sourceList = Arrays.asList(
                new Tuple2<Integer, String>(65, "leo"),
                new Tuple2<Integer, String>(59, "tom"),
                new Tuple2<Integer, String>(100, "marry"),
                new Tuple2<Integer, String>(80, "jack")
        );

        //并行化

        JavaPairRDD<Integer, String> sources = sc.parallelizePairs(sourceList);

        // 对 sources RDD  执行 sortByKey 算子
        // sortByKey 其实就是根据 key 进行排序,可以手动指定升序还是降序
        // 返回的,还是 JavaPairRDD 其中的元素内容,就是和原来RDD 一模一样
        // 只是RDD 的中的元素顺序不一样了.
        JavaPairRDD<Integer, String> sortedScores = sources.sortByKey(false);

        sortedScores.foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2));

        //关闭 JavaSparkContext
        sc.stop();

    }

    /**
     * join: 按照学生的分数进行排序
     */
    public static void join() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "tom"),
                new Tuple2<Integer, String>(3, "marry"),
                new Tuple2<Integer, String>(4, "jack")
        );

        List<Tuple2<Integer, Integer>> sourceList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 88),
                new Tuple2<Integer, Integer>(2, 60),
                new Tuple2<Integer, Integer>(3, 100),
                new Tuple2<Integer, Integer>(4, 59)
        );

        //并行化

        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);

        JavaPairRDD<Integer, Integer> sources = sc.parallelizePairs(sourceList);


        // 使用join 算子关联两个 RDD
        // join 以后,还是会根据key 进行 join ,并返回 JavaPairRDD
        // 但是 JavaPairRDD 的第一个字符串类型是之前两个 JavaPairRDD 的key类型,因为是通过 key 进行join 的.
        // 第二个泛型,是Tuple<v1,v2> 的类型,Tuple2 的两个泛型分别是原始RDD 的value 的类型.
        // join 就是返回的RDD 的每一个元素,就是通过 key join 上的一个part
        // 比如现在有一个(1,1)(1,2),(1,3) 的 RDD
        // 还有一个 (1,4),(2,1),(3,1) 的 RDD
        // join以后实际会得到 (1,(1,4)),(1,(2,4)),(1,(3,4)) 这么个 RDD

        // a 去join b ,则 a 的数据在前面
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentSources = students.join(sources);

//        JavaPairRDD<Integer, Tuple2<Integer, String>> studentSources = sources.join(students);
        studentSources.foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2._1 + ":" + tuple2._2._2));

        //关闭 JavaSparkContext
        sc.stop();

    }
}
