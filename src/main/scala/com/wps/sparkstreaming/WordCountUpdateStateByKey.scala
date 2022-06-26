package com.wps.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountUpdateStateByKey {

  //对该函数取个名字
  val myReduce = (values: Seq[Int], state: Option[Int]) => {
    val currentCount: Int = values.sum
    val lastCount: Int = state.getOrElse(0)
    Some(currentCount + lastCount)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    //local[*] 你当前的电脑有多少个cpu core * 就代表是几
    conf.setMaster("local[2]")
    conf.setAppName("wordcount")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("E:\\DataFiles\\kkb_data\\sparkstreaming\\UpdateStateByKey")

    val dateDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val wordDStream: DStream[String] = dateDStream.flatMap(_.split(","))
    val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    /**
     * Option
     *  Some(有值)
     *  None(无值)
     * updateFunc: (Seq[V], Option[S]) => Option[S]
     * scala:
     *  方法:
     *   def
     *  函数：
     *    =>
     */
    val result: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey(myReduce)

    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
