package com.core

import org.apache.spark.SparkContext
import org.apache.log4j._

object avgByYear {

  // Map the values and split age // numFriends
  def parseValue(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }

  def main(args: Array[String]): Unit = {
    // Get Log
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create the spark context and load the dataset
    val sc = new SparkContext("local[*]","avgByYear")
    val dataset = sc.textFile("../../datasets/fakefriends.csv")
    val rdd = dataset.map(parseValue)

    val totalByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y)=> (x._1 + y._1, x._2 + y._2))
    val avgByYear = totalByAge.mapValues( x => x._1 / x._2)

    avgByYear.sortByKey().collect().foreach(println)



  }
}
