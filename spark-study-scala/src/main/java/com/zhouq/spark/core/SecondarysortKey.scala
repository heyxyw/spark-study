package com.zhouq.spark.core

/**
  * 二次排序key
  *
  * @Author: zhouq
  * @Date: 2019-08-20
  */
class SecondSortKey(val first: Int,val secound: Int) extends Ordered[SecondSortKey] with Serializable {

  override def compare(that: SecondSortKey): Int = {
    if (this.first - that.first != 0){
      this.first  - that.first
    }else{
      this.secound - that.secound
    }
  }
}
