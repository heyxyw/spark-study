package com.zhouq.spark.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义二次排序
 *
 * @Author: zhouq
 * @Date: 2019-08-20
 */
public class SecondarysortKey implements Ordered<SecondarysortKey> , Serializable {

    //首先自定义key 里面需要进行排序的列
    private int first;
    private int secondary;


    //先比较第一列，再比较第二列。

    @Override
    public boolean $greater(SecondarysortKey other) {
        if (this.first > other.getFirst()){
            return true;
        }else if(this.first == other.getFirst() &&
                this.secondary > other.getSecondary()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarysortKey other) {
        if (this.$greater(other)){
            return true;
        }else if(this.first == other.getFirst() && this.secondary ==other.getSecondary()){
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondarysortKey other) {

        if (this.first - other.getFirst() != 0){
            return this.first - other.getFirst();
        }else{
             return this.secondary - other.getSecondary();
        }
    }

    @Override
    public int compareTo(SecondarysortKey other) {

        if (this.first - other.getFirst() != 0){
            return this.first - other.getFirst();
        }else{
            return this.secondary - other.getSecondary();
        }
    }

    @Override
    public boolean $less(SecondarysortKey other) {

        if (this.first < other.getFirst()){
            return true;
        }else if(this.first == other.getFirst() && this.secondary < other.getSecondary()){
            return true;
        }

        return false;
    }


    @Override
    public boolean $less$eq(SecondarysortKey other) {
        if (this.$less(other)){
            return true;
        }else if (this.first == other.getFirst() && this.secondary == other.getSecondary()){
            return true;
        }

        return false;
    }


    public SecondarysortKey(int first, int secondary) {
        this.first = first;
        this.secondary = secondary;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecondary() {
        return secondary;
    }

    public void setSecondary(int secondary) {
        this.secondary = secondary;
    }
}
