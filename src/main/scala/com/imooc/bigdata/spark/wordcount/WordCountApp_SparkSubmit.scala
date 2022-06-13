package com.imooc.bigdata.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp_SparkSubmit {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile(args(0))
    rdd.flatMap(_.split(" ")).map(word=>(word,1))
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))
    sc.stop()
  }

}
