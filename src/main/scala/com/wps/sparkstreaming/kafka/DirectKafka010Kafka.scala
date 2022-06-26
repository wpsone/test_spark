//package com.wps.sparkstreaming.kafka
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.{SPARK_BRANCH, SparkConf}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object DirectKafka010Kafka {
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
//    //步骤一：获取配置信息
//    val conf: SparkConf = new SparkConf().setAppName("DirectKafka010Kafka").setMaster("local[3]")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val ssc = new StreamingContext(conf, Seconds(2))
//
//    val brokers = "node01:9092"
//    val topics = "flink"
//    val groupId = "flink_consumer10"
//
//    val topicsSet: Set[String] = topics.split(",").toSet
//
//    val kafkaParams = Map[String,Object](
//      "bootstrap.servers" -> brokers,
//      "group.id" -> groupId,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer]
//    )
//
//    //步骤二：获取数据源
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
//    )
//
//    //设置监听器
//    ssc.addStreamingListener(new OffsetListener10(stream))
//
//    val result: DStream[(String, Int)] = stream.map(_.value())
//      .flatMap(_.split(","))
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
