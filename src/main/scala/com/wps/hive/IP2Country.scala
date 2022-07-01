package com.wps.hive

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}

import java.sql.{DriverManager, PreparedStatement}

case class Login(logtime:String,account_id:String,country:String,province:String,pt:String)

object IP2Country {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    val spark: SparkSession = SparkSession.builder()
      .appName("Ip2Country")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //获取用户登录表
    val data_login: DataFrame = spark.sql("select logtime,account_id,ip" +
      ",(cast(split(ip,\"\\\\.\")[0] as bigint)*256*256*256+cast(split(ip,\"\\\\.\")[1] as bigint)*256*256+cast(split(ip,\"\\\\.\")[2] as bigint)*256+cast(split(ip,\"\\\\.\")[3] as bigint)) as ipLong " +
      ",pt from myhive.wm_login_data_day")
    data_login.show(10)

    data_login.foreachPartition(iter =>{
      val driverName="org.apache.hive.jdbc.HiveDriver"
      val url = "jdbc:hive2://node03:10000/myhive"
      Class.forName(driverName)
      val conn = DriverManager.getConnection(url)

//      val set = Set[Login]()
      try {
        //获取ip_china数据
//        val selectsql = "select country,province from wm_ip_china where long_ip_start<= ? and long_ip_end>= ?"
//        val stmt: PreparedStatement = conn.prepareStatement(selectsql)
        //批量插入登录-地区表
        val insertsql = "insert into myhive.wm_login_region_day partition(pt= ? ) select ? as logtime,? as account_id,country,province from wm_ip_china where long_ip_start<= ? and long_ip_end>= ? "
        val stmtin: PreparedStatement = conn.prepareStatement(insertsql)

        iter.foreach(line=>{
          stmtin.setString(1,line.get(4).toString)
          stmtin.setString(2,line.get(0).toString)
          stmtin.setString(3,line.get(1).toString)
          stmtin.setLong(4,line.get(3).toString.toLong)
          stmtin.setLong(5,line.get(3).toString.toLong)
          stmtin.execute()
        })

      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        if (conn!=null) conn.close()
      }

    })

    spark.stop()
  }

}

