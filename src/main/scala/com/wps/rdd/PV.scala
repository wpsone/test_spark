package com.wps.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO:利用spark实现点击日志分析——>PV
object PV {
  def main(args: Array[String]): Unit = {
    //1、构建SparkConf
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2、构建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、读取数据文件
    val data: RDD[String] = sc.textFile("E:\\DataFiles\\access.log")

    //4、统计PV
    val pv: Long = data.count()
    println("pv:" + pv)

    sc.stop()
  }
}
