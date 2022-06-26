//package com.wps.sparkstreaming.kafka
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.Decoder
//import org.apache.spark.{SparkContext, SparkException}
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.api.java.{JavaPairInputDStream, JavaStreamingContext}
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
//import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}
//
//import java.util.{Map => JMap, Set => JSet}
//import scala.collection.JavaConverters._
//import scala.reflect.ClassTag
//
////自己管理offset
//class KafkaManager(val kafkaParams:Map[String,String]) extends Serializable {
//
//  private val kc = new KafkaCluster(kafkaParams)
//
//  //创建数据流
//  def createDirectStream[K: ClassTag, V: ClassTag,KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag] (
//                                    ssc: StreamingContext,
//                                    kafkaParams: Map[String,String],
//                                    topics: Set[String]): InputDStream[(K,V)] = {
//    val groupId = kafkaParams.get("group.id").get
//    //在zookeeper上读取offsets前先根据实际情况更新offsets
//    setOrUpdateOffsets(topics,groupId)
//
//    //从zookeeper上读取offset开始消费message
//    val messages = {
//      val partitionsE: Either[Err, Set[TopicAndPartition]] = kc.getPartitions(topics)
//      if (partitionsE.isLeft) {
//        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
//      }
//      val partitions: Set[TopicAndPartition] = partitionsE.right.get
//      val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = kc.getConsumerOffsets(groupId, partitions)
//      if (consumerOffsetsE.isLeft) {
//        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
//      }
//      //我们上一次消费的偏移量的位置
//      val consumerOffsets: Map[TopicAndPartition, Long] = consumerOffsetsE.right.get
//      //还是调用的sparkStermaing的API
//      KafkaUtils.createDirectStream(
//        ssc, kafkaParams, consumerOffsets,(mmd: MessageAndMetadata[K,V])=> (mmd.key(),mmd.message())
//      )
//    }
//    messages
//  }
//
//  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
//                                    jssc: JavaStreamingContext,
//                                    keyClass: Class[K],
//                                    valueClass: Class[V],
//                                    keyDecoderClass: Class[KD],
//                                    valueDecoderClass: Class[VD],
//                                    kafkaParams: JMap[String, String],
//                                    topics: JSet[String]
//                                            ): JavaPairInputDStream[K, V] = {
//    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
//    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
//    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
//    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
//
//    createDirectStream[K, V, KD, VD](jssc.ssc,  Map(kafkaParams.asScala.toSeq: _*),
//      Set(topics.asScala.toSeq: _*));
//  }
//
//  //创建数据流前，根据实际消费情况更新消费offsets
//  private def setOrUpdateOffsets(topics: Set[String],groupId: String): Unit = {
//    topics.foreach(topic => {
//      var hasConsumed = true
//      val partititonsE = kc.getPartitions(Set(topic))
//      if (partititonsE.isLeft)
//        throw new SparkException(s"get kafka partition failed: ${partititonsE.left.get}")
//      val partititons = partititonsE.right.get
//      val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = kc.getConsumerOffsets(groupId, partititons)
//      if (consumerOffsetsE.isLeft) hasConsumed = false;
//      if (hasConsumed) {// 消费过
//        /**
//          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
//          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
//          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
//          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
//          * 这时把consumerOffsets更新为earliestLeaderOffsets
//          */
//        val earliestLeaderOffsetsE: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getEarliestLeaderOffsets(partititons)
//        if (earliestLeaderOffsetsE.isLeft)
//          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
//        val earliestLeaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestLeaderOffsetsE.right.get
//        val consumerOffsets: Map[TopicAndPartition, Long] = consumerOffsetsE.right.get
//
//        var offsets: Map[TopicAndPartition,Long] = Map()
//        consumerOffsets.foreach({case(tp,n) =>
//          val earliestLeaderOffset: Long = earliestLeaderOffsets(tp).offset
//          if (n < earliestLeaderOffset) {
//            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition + " offsets已经过时，更新为" + earliestLeaderOffset)
//            offsets += (tp -> earliestLeaderOffset)
//          }
//        })
//        if (!offsets.isEmpty) {
//          kc.setConsumerOffsets(groupId,offsets)
//        }
//      } else {// 没有消费过
//        val reset: Option[String] = kafkaParams.get("auto.offset.reset").map(_.toLowerCase())
//        var leaderOffsets: Map[TopicAndPartition,LeaderOffset] = null
//        if (reset == Some("smallest")) {
//          val leaderOffsetsE: Either[Err, Map[TopicAndPartition, LeaderOffset]] = kc.getEarliestLeaderOffsets(partititons)
//          if (leaderOffsetsE.isLeft)
//            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
//          leaderOffsets = leaderOffsetsE.right.get
//        } else {
//          val leaderOffsetsE: Either[Err, Map[TopicAndPartition, LeaderOffset]] = kc.getLatestLeaderOffsets(partititons)
//          if (leaderOffsetsE.isLeft) {
//            throw new SparkException(s"gt latest leader offsets failed: ${leaderOffsetsE.left.get}")
//          }
//          leaderOffsets = leaderOffsetsE.right.get
//        }
//        val offsets: Map[TopicAndPartition, Long] = leaderOffsets.map {
//          case (tp, offset) => (tp, offset.offset)
//        }
//        kc.setConsumerOffsets(groupId,offsets)
//      }
//    })
//  }
//}
