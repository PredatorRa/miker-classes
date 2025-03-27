package com.miker.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark01_RDD_Operator_Transform
 *
 * @author chenyuqian
 * @version 1.0
 *          Created on 2025/3/27 11:26
 * */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[Int] = rdd.map(_*2)

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
