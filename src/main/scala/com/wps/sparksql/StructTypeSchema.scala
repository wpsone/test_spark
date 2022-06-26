package com.wps.sparksql


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\DevSoft\\winutils")
    //1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()

    //2、获取SparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    //3、读取文件数据
    val data: RDD[Array[String]] = sc.textFile("E:\\DataFiles\\kkb_data\\person.txt").map(x => x.split(" "))

    //4、将rdd与row对象关联
    val rowRDD: RDD[Row] = data.map(x => Row(x(0), x(1), x(2).toInt))

    //5、指定dataFrame的schema信息
    //这里指定的字段个数和类型必须要跟Row对象保持一致
    val schema: StructType = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil
    )

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame.printSchema()
    dataFrame.show()

    dataFrame.createTempView("user")
    spark.sql("select * from user").show()

    spark.stop()
  }

}
