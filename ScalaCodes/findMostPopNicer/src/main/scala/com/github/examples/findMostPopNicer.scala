package com.github.examples

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}


object findMostPopNicer extends  App{
  def loadMoviesName(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("../../datasets/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "finMostPopNicer")
  var nameDict = sc.broadcast(loadMoviesName)

  val lines = sc.textFile("../../datasets/ml-100k/u.data")
  val rdd = lines.map(x => (x.split("\t")(1).toInt, 1)).reduceByKey((x,y) => x+y)
  val flippedAndSorted = rdd.map(x => (x._2,x._1)).sortByKey()
  val sortedWithName = flippedAndSorted.map(x => (nameDict.value(x._2), x._1))
  val results = sortedWithName.collect()
  results.foreach(println)

}
