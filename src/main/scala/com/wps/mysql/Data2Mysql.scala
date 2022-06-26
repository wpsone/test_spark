package com.wps.mysql

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

//todo:通过sparksql把结果数据写入到mysql表中
object Data2Mysql {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    //1、创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Data2Mysql")
      .master("local[2]")
      .getOrCreate()

    //2、读取mysql表中数据
    //2.1 定义url连接
    val url = "jdbc:mysql://localhost:3306/spark?serverTimezone=Asia/Shanghai"
    //2.2 定义表名
    val table = "user"
    //2.3 定义属性
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123123")

    val mysqlDF: DataFrame = spark.read.jdbc(url, table, properties)
    //把dataFrame注册成一张表
    mysqlDF.createTempView("user")
    //通过sparkSession调用sql方法
    //需要统计经度和维度出现的人口数大于1000的记录，保存到mysql中
    val result: DataFrame = spark.sql("select * from user where age>30")

    //保存结果数据到mysql表中
    //mode:指定数据的插入模式
    //overwrite:表示覆盖，如果表不存在，事先帮我们创建
    //append:表示追加，如果表不存在，事先帮我们创建
    //ignore:表示忽略，如果表事先存在，就不进行任何操作
    //error:如果表事先存在就报错（默认选项）
    result.write.mode("error").jdbc(url,"kaikeba",properties)

    //关闭
    spark.stop()
  }
}
