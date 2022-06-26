package com.wps.udf

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLFunction {

  def main(args: Array[String]): Unit = {
    //1、创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSQLFunction").master("local[2]").getOrCreate()

    //2、构建数据源生成DataFrame
    val dataFrame: DataFrame = sparkSession.read.text("E:\\DataFiles\\kkb_data\\test_udf_data.txt")

    //3、注册表
    dataFrame.createTempView("t_udf")

    //4、实现自定义UDF函数
    //小写转大写
    sparkSession.udf.register("low2Up",new UDF1[String,String](){
      override def call(t1: String): String = {
        t1.toUpperCase()
      }
    },StringType)

    //大写转小写
    sparkSession.udf.register("up2low",(x:String)=>x.toLowerCase())

    //5、把数据文件中的单词统一转换成大小写
    sparkSession.sql("select value from t_udf").show()
    sparkSession.sql("select low2Up(value) from t_udf").show()
    sparkSession.sql("select up2low(value) from t_udf").show()

    sparkSession.stop()
  }

}
