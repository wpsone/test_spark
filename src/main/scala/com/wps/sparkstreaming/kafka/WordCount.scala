//package com.wps.sparkstreaming.kafka
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object WordCount {
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
//    val conf = new SparkConf().setMaster("local[3]").setAppName("wordcount")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val ssc = new StreamingContext(conf, Seconds(10))
//
//    val brokers = "node01:9092"
//    val topics = "flink"
//    val groupId = "flink_consumer" //消费者的名字
//
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String,String](
//      "metadata.broker.list"-> brokers,
//      "group.id"->groupId
//    )
//
//    //关键步骤一：设置监听器，完成偏移量提交
//    //监听器作用：每运行完一个批次，就提交一次偏移量
//    ssc.addStreamingListener(new OffsetListener(kafkaParams))
//    //关键步骤二： 创建对象，然后通过这个对象获取到上次的偏移量，然后获取到数据流
//    val km = new KafkaManager(kafkaParams)
//    //1。 获取到流，这个流里面是offset的信息的
//    val messges: InputDStream[(String, String)] = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//
//      //offset的信息就会丢失了
//      val result: DStream[(String, Int)] = messges.map(_._2)
//        .flatMap(_.split(","))
//        .map((_, 1))
//        .reduceByKey(_ + _)
//
//    //2。直接对上面获取的流做foreachRDD的操作
//    result.foreachRDD(rdd =>
//      //缺点就是，所有的业务逻辑都要在这儿实现
//      //不是说不行，也是可以的。
//      //但是大家会发现，那我们的DSteam编程，就变成了SparkCore编程
//      //如果功能就是单词计数，问题也不大。
//      //但是如果你想使用一些DStream特有的算子，你就用不了
//      //UpdateStateBykey mapwithstate ,tansform,Window(窗口)
//      //实际上这个时候，里面已经没有offset的信息了
//      //那你就没办法提交offset
//      rdd.foreach(line => {
//        println(line._1 + " " + line._2)
//        println("==================")
//        //代码到这儿 应该要提交一下偏移量了。
//        //确实是可以实现提交offset的功能的。
//      })
//    )
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
