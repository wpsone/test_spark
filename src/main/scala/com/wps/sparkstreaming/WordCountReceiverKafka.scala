//package com.wps.sparkstreaming
//
//import kafka.serializer.StringDecoder
//import org.apache.commons.codec.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.immutable
//
//object WordCountReceiverKafka {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountReceiverKafka")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val kafkaParams = Map[String,String](
//      "zookeeper.connect" -> "node01:2181,node02:2181,node03:2181",
//      "group.id" -> "testflink"
//    )
//
//    val topics = "flink".split(",").map((_,1)).toMap
//
//    val lines: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER
//    )
//
////    val kafkaStreams: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(_ => {
////      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
////        ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER
////      )
////    })
////    val lines: DStream[(String, String)] = ssc.union(kafkaStreams)
//
//    lines.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//}
