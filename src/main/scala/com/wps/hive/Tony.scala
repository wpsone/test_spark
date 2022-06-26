package com.wps.hive

import org.apache.spark.sql.SparkSession

object Tony {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    val spark: SparkSession = SparkSession.builder()
      .appName("Tony")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from myhive.stu").show()
    spark.stop()
  }

}
