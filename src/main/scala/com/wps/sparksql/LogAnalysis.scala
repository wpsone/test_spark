package com.wps.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * 日志案例分析
 */
object LogAnalysis {

  //定义url连接
  val url = "jdbc:mysql://node03:3306/spark"
  //定义属性
  val properties = new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123123")

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    //1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("LogAnalysis").setMaster("local[2]")

    //2、构建sparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3、获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    //4、读取数据文件
    val logRDD: RDD[String] = sc.textFile("./logs/access.log")

    //5、过滤脏数据，然后解析
    val rightRDD: RDD[String] = logRDD.filter(line => AccessLogUtils.isValidateLogLine(line))
    val accessLogRDD: RDD[AccessLog] = rightRDD.map(line => AccessLogUtils.parseLogLine(line))

    //6、将RDD转换成DataFrame
    import spark.implicits._
    val accessLogDF: DataFrame = accessLogRDD.toDF

    //7、将DataFrame注册成一张表
    accessLogDF.createTempView("access")

    //todo:8、指标分析
    //todo:8.1 求contentSize的平均值，最大值以及最小值
    val result1: DataFrame = spark.sql(
      """
        |select
        |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time,
        |avg(contentSize) as avg_contentSize,
        |max(contentSize) as max_contentSize,
        |min(contentSize) as min_contentSize
        |from access
        |""".stripMargin)

    result1.show()
    result1.write.jdbc(url,"t_contentSizeInfo",properties)

    //todo:8.2 求pv和uv
    val result2: DataFrame = spark.sql(
      """
        |select
        |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time,
        |count(*) as pv,
        |count(distinct ipAddress) as uv
        |from access
        |""".stripMargin)
    result2.show()
    result2.write.jdbc(url,"t_uv_pv",properties)

    //todo:8.3 求各个响应码出现的次数
    val result3 = spark.sql(
      """
        |select
        |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time,
        |responseCode as code,
        |count(*) as count
        |from access
        |group by responseCode
          """.stripMargin)

    //展示结果数据
    result3.show()

    //保存结果数据到mysql表中
    result3.write.jdbc(url,"t_responseCode",properties)

    //todo:8.4 求访问url次数最多的前N位
    val result4 = spark.sql(
      """
        |select
        |*,date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time
        |from (
        |select
        |url as url,
        |count(*) as count
        |from access
        |group by url) t
        |order by t.count desc limit 5
          """.stripMargin)

    //展示结果数据
    result4.show()

    //保存结果数据到mysql表中
    result4.write.jdbc(url,"t_url",properties)

    //todo:8.5 求各个请求方式出现的次数
    val result5 = spark.sql(
      """
        |select
        |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time,
        |method as method,
        |count(*) as count
        |from access
        |group by method
          """.stripMargin)

    //展示结果数据
    result5.show()

    //保存结果数据到mysql表中
    result5.write.jdbc(url,"t_method",properties)

    spark.stop()
  }
}
