//package com.wps.sparkstreaming
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object WordCountDirectKafka010 {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val conf: SparkConf = new SparkConf().setAppName("WordCountDirectKafka010").setMaster("local[5]")
//    conf.set("spark.streaming.kafka.maxRatePerPartition","5")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val brokers = "node01:9092"
//    val topics = "flink"
//    val groupId = "flink"
//
//    val topicSet: Set[String] = topics.split(",").toSet
//    val kafkaParams: Map[String, Object] = Map[String, Object](
//      "bootstrap.servers" -> brokers,
//      "group.id" -> groupId,
//      "fetch.message.max.bytes" -> "209715200",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer]
//    )
//
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
//    )
//
//    val result: DStream[(String, Int)] = stream.map(_.value()).flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//}
