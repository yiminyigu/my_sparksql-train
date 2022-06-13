package com.imooc.bigdata.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("SparkWordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("file:///Users/stevecai/Desktop/2022Big_Data/Imooc385-SparkSQL/my_sparksql-train/src/main/scala/com/imooc/bigdata/spark/wordcount/wordcount.txt")
    rdd.flatMap(_.split(" ")).map(word=>(word,1))
      .reduceByKey(_+_)
      .map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
        .saveAsTextFile("file:///Users/stevecai/Desktop/2022Big_Data/Imooc385-SparkSQL/my_sparksql-train/src/main/scala/com/imooc/bigdata/spark/wordcount/out")
      //.collect().foreach(println)

    sc.stop()
  }

}
