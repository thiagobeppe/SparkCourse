package com.github.scala.examples


import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext


object WordCount extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  //take the context and load the data
  val sc = new SparkContext("local[*]", "wordCount")
  val rdd =  sc.textFile("../../datasets/book.txt")

  //Apply some pre processing transforming the data to lowercase and remove numbers and punctuation with a regular
  // expressions and splitting to count
  val words = rdd.map(x => x.toLowerCase)
              .map(x => x.replaceAll("[^\\w\\s]|\\d+", ""))
              .flatMap(x => x.split(" "))

  //Making a reduce
  val wordCounts = words.map(x => (x,1)).reduceByKey((x,y) => x+y)

  //Sorting by number of cases
  val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

  for(result <- wordCountsSorted){
    println(result)
  }
}


