package com.github.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object findMostPop extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc =  new SparkContext("local[*]", "findMostPop")
  val file = sc.textFile("../../datasets/ml-100k/u.data")

  val rdd = file.map(x => (x.split("\t")(1).toInt, 1))
  val rddReduced = rdd.reduceByKey((x,y) => x+y)
  val flippedAndSorted = rddReduced.map(x => (x._2,x._1)).sortByKey()
  val results = flippedAndSorted.collect()

  results.foreach(println)
}
