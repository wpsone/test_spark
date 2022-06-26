import org.apache.spark.{SparkConf, SparkContext}

object test01 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Data2Hbase").setMaster("local[2]")

    //2、构建SparkContext
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //cogroup
    val rdd3 = rdd1.cogroup(rdd2)
    //groupbykey
    val rdd4 = rdd1.union(rdd2).groupByKey
    //注意cogroup与groupByKey的区别
    rdd3.foreach(println)
    rdd4.foreach(println)
  }

}
