package com.miker.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(
      List(1, 2, 3, 4), 2
    )

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
