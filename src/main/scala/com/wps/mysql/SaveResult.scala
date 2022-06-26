package com.wps.mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:sparksql可以吧结果数据保存到不同的外部存储介质中
object SaveResult {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")

    //2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3、加载数据源
    val jsonDF: DataFrame = spark.read.json("E:\\DataFiles\\kkb_data\\score.json")

    //4、把DataFrame注册成表
    jsonDF.createTempView("t_score")

    //5、统计分析
    val result: DataFrame = spark.sql("select * from t_score where score > 80")

    //保存结果数据到不同的外部存储介质中
    //todo: 5.1 保存结果数据到文本文件  ----  保存数据成文本文件目前只支持单个字段，不支持多个字段
    result.select("name").write.text("./data/result/123.txt")

    //todo: 5.2 保存结果数据到json文件
    result.write.json("./data/json")

    //todo: 5.3 保存结果数据到parquet文件
    result.write.parquet("./data/parquet")

    //todo: 5.4 save方法保存结果数据，默认的数据格式就是parquet
    result.write.save("./data/save")

    //todo: 5.5 保存结果数据到csv文件
    result.write.csv("./data/csv")

    //todo: 5.6 保存结果数据到表中
    result.write.saveAsTable("t1")

    //todo: 5.7  按照单个字段进行分区 分目录进行存储
    result.write.partitionBy("classNum").json("./data/partitions")

    //todo: 5.8  按照多个字段进行分区 分目录进行存储
    result.write.partitionBy("classNum","name").json("./data/numPartitions")

    spark.stop()
  }
}
