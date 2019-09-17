package com.zhouq.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

/**
  * Create by zhouq on 2019/9/17
  * 自定义函数，累计
  *
  */
class UDAF_StringCount extends UserDefinedAggregateFunction {

  // inputSchema 指的是输入数据类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str", StringType, true)))
  }

  //bufferSchema 指中间进行聚合时，所处理的数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))
  }

  // dateType 指返回值的类型
  override def dataType: DataType = {
    IntegerType
  }

  override def deterministic: Boolean = {
    true
  }

  // 为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  // 指的是，每个分组，有新的值进来的时候，如何进行分组对应的聚合值的运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //在原来的值上加一
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //由于Spark 是分布式执行的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  // 但是最后一个分组，在各节点上的聚合值，要进行merge ，就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  // 指的是一个分组的聚合值，如何通过中间的缓存聚合值，返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
