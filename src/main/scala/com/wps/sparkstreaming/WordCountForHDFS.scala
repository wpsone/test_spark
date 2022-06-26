package com.wps.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountForHDFS {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))

    //HA高可用模式，kkb是逻辑名，把core-site.xml hdfs-site.xml放到resources目录下
    val hdfsDStream: DStream[String] = ssc.textFileStream("hdfs://kkb/hello/")
    val result: DStream[(String, Int)] = hdfsDStream.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
