package com.wps.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountBlackFilter {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(List("?", "!"))
      .map(word => (word, true))

    val blackListBroadcast: Broadcast[Array[(String, Boolean)]] = ssc.sparkContext.broadcast(blackListRDD.collect())

    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val wordDStream: DStream[String] = dataDStream.flatMap(_.split(","))
    val wordAndOneDSteam: DStream[(String, Int)] = wordDStream.map((_, 1))

    val filterDStream: DStream[(String, Int)] = wordAndOneDSteam.transform(rdd => {
      val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blackListBroadcast.value)
      val filterRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blackListRDD)

      val result: RDD[(String, (Int, Option[Boolean]))] = filterRDD.filter(tuple => {
        tuple._2._2.isEmpty
      })
      val wordAndOne: RDD[(String, Int)] = result.map(tuple => {
        (tuple._1, tuple._2._1)
      })
      wordAndOne
    })

    filterDStream.reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
