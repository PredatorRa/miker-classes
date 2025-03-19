package com.miker.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark01_WordCount
 *
 * @author chenyuqian
 * @version 1.0
 *          Created on 2025/1/28 14:41
 * */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data")

    val word = lines.flatMap(_.split(" ")).map(w=>(w,1))

    val wordCount: RDD[(String, Int)] = word.reduceByKey((x, y) => x + y)

    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }

}
