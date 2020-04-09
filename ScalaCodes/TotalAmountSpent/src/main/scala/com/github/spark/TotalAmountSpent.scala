package com.github.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpent extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Function to extract from data only the things what i need
  def parseLines(files: String): (Int, Float) ={
    val lines = files.split(",")
    (lines(0).toInt,lines(2).toFloat)
  }

  //Def our sparkContext
  val sc = new SparkContext("local[*]", "TotalAmountSpent")
  //Read a file
  val rdd = sc.textFile("../../datasets/customer-orders.csv")
  //Apply the function to extract Uid and amount
  val parsedRdd = rdd.map(parseLines)
  //Make a reduce to sum all the amount
  val reduced = parsedRdd.reduceByKey( (x,y)=> x+y)
  //Transform the value in key to sort by it
  val flipped = reduced.map(x => (x._2,x._1))
  //Sort it
  val totalResultsSorted = flipped.sortByKey()
  val results = totalResultsSorted.collect()

  //Print in the console a formatted string
  results.foreach((lines) => {
    val uid = lines._2
    val value = lines._1
    val formattedValue = f"$value%.02f"
    println(s"User: $uid. Value: $formattedValue")
  })


}
