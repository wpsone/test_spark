package com.wps.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {
  def main(args: Array[String]): Unit = {
    //1、构建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    //2、构建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、读取数据文件
    val data: RDD[String] = sc.textFile("E:\\DataFiles\\access.log")

    //4、切分每一行，获取第一个元素 即ip
    val ips: RDD[String] = data.map(x => x.split(" ")(0))

    //5、按照ip去重
    val distinctRDD: RDD[String] = ips.distinct()

    //6、统计uv
    val uv: Long = distinctRDD.count()
    println("uv:"+uv)

    sc.stop()
  }
}
