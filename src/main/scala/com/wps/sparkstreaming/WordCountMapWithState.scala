package com.wps.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}

object WordCountMapWithState {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("wordcount")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("E:\\DataFiles\\kkb_data\\sparkstreaming\\MapWithState")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val words: DStream[String] = lines.flatMap(_.split(","))
    val wordDStream: DStream[(String, Int)] = words.map(x => (x, 1))

    val initialRDD: RDD[(String, Long)] = sc.parallelize(List(("hadoop", 100L), ("storm", 32L)))


    val stateSpec = StateSpec.function((currentTime:Time,
                        key:String,
                        value:Option[Int],
                        lastState:State[Long])=>{
      val sum = value.getOrElse(0).toLong + lastState.getOption().getOrElse(0L)
      val output: (String, Long) = (key, sum)
      if (!lastState.isTimingOut()) {
        lastState.update(sum)
      }
      Some(output)
    })
      .initialState(initialRDD)
//      .numPartitions(2)
      .timeout(Seconds(15))

    val result: MapWithStateDStream[String, Int, Long, (String, Long)] = wordDStream.mapWithState(stateSpec)
    result.print() //只打印当前 更新key的结果
//    result.stateSnapshots().print() //打印所有key的结果

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
