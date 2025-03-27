package com.miker.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark02_RDD_File_Par
 *
 * @author chenyuqian
 * @version 1.0
 *          Created on 2025/3/27 11:08
 * */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("data/1.txt")

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
