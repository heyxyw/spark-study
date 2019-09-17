package com.zhouq.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;

/**
 * 使用反射的方式将RDD 映射成 DataFrame
 * Create by zhouq on 2019/9/7
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("H:\\bigdatatest\\spark\\students.txt");

        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                Student student = new Student();
                student.setId(Integer.valueOf(lineSplited[0].trim()));
                student.setName(lineSplited[1].trim());
                student.setAge(Integer.valueOf(lineSplited[2].trim()));
                return student;
            }
        });

        //使用反射方式，将RDD 转换为 DataFreme
        //将Student.class 传入进去，其实就是用反射的方式来创建 DataFrame
        //因为 Student.class 本身就是反射的一个应用
        //然后底层还是得通过 Student Class 进行反射，来获取 其中的field
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);

        // 拿到 一个DataFrame 以后，注册为一个临时表，然后针对其中的数据执行 SQL 语句
        studentDF.registerTempTable("students");

        // 针对 students 临时表，执行sql 语句，查询年龄小于等于 18岁的学生。就是 teenageer
        DataFrame teenageerDF = sqlContext.sql("select * from students where  age <= 18");

        // 将查询出来的 DataFrame 再次转化为 RDD
        JavaRDD teenagerRDD = teenageerDF.toJavaRDD();

        // 将RDD中的数据，进行映射成Student

        /**
         * 注意： 在Java 中，Row 中的列的数据可能会跟我们期望的顺序不一致
         * 可以使用row.getAs(fieldName) 来获取值
         *
         */
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                System.out.println(row);
                Student stu = new Student();
                stu.setId(row.getInt(0));
                stu.setName(row.getString(2));
                stu.setAge(row.getInt(1));
                return stu;
            }
        });

        // 把数据 collect 回来，打印一下
        List<Student> studentList = teenagerStudentRDD.collect();

        for (Student student : studentList) {
            System.out.println(student);
        }

        sc.stop();
    }


    /**
     * Java Bean
     */
    public static class Student implements Serializable {
        private int id;
        private String name;
        private int age;

        public Student() {

        }

        public Student(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
