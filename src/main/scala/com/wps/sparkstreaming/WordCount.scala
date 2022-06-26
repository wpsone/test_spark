package com.wps.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//数据源：从socket直接接收数据，模拟从Kafka接收数据的效果
case object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    //设置日志格式
    Logger.getLogger("org").setLevel(Level.ERROR)
    //第一步：数据输入
    //1.初始化程序入口
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    //2.获取数据
    val dataDStream: DStream[String] = ssc.socketTextStream("localhost", 8888,StorageLevel.MEMORY_AND_DISK_SER_2)

    //第二步：数据处理
    //3.数据处理
    val wordDStream: DStream[String] = dataDStream.flatMap(_.split(","))
    val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val resultDStream: DStream[(String, Int)] = wordAndOneDStream.reduceByKey(_ + _)

    //第三步：数据输出
    //4.数据输出
    resultDStream.print()
    //5.启动实时任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
