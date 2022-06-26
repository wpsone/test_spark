package com.wps.hive

import org.apache.spark.sql.SparkSession

//todo:利用sparksql操作hivesql
object HiveSupport {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    //1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()
    //2、直接使用sparkSession去操作hivesql语句

    //2.1 创建一张hive表
    spark.sql("create table people(id string,name string,age int) row format delimited fields terminated by ','")

    //2.2 加载数据到hive表中
    spark.sql("load data local inpath '/E:/IdeaProjects/test_spark/data_hive/hivesql.txt' into table people")

    //2.3 查询
    spark.sql("select * from people").show()

    spark.stop()
  }
}
