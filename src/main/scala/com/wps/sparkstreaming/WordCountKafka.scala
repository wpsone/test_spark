//package com.wps.sparkstreaming
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object WordCountKafka {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val conf = new SparkConf()
//    conf.setAppName("wordcount")
//    conf.setMaster("local[2]")
//
//    val ssc = new StreamingContext(conf, Seconds(2))
//
//    /**
//     * [
//    K: ClassTag,
//    V: ClassTag,
//    KD <: Decoder[K]: ClassTag,
//    VD <: Decoder[V]: ClassTag] (
//      ssc: StreamingContext,
//      kafkaParams: Map[String, String],
//      topics: Set[String]
//  ): InputDStream[(K, V)]
//     */
//    val kafkaParams = Map("metadata.broker.list" -> "node01:9092")
//    val topics: Set[String] = "hadoop,hive".split(",").toSet
//    val lineDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
//
//    val result: DStream[(String, Int)] = lineDStream.flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
