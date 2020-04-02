package com.github.examples

import breeze.linalg.{max, min}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object findingTemperature extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLines(line: String): (String,String,Float) = {
    val parsed = line.split(",")
    val stationId = parsed(0)
    val entryType = parsed(2)
    val temp = parsed(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
   (stationId,entryType,temp)
  }

  val sc = new SparkContext("local[*]", "findingTemperature")
  val files = sc.textFile("../../datasets/1800.csv")

  def forMinimum(file: RDD[String]) ={
    val rdd = file.map(parseLines)
    val filteredRdd =  rdd.filter(x => x._2 == "TMIN")
    val stationTemps = filteredRdd.map(x => (x._1, x._3.toFloat))
    val minTempByStation = stationTemps.reduceByKey((x,y)=> min(x,y))

    val results = minTempByStation.collect()
    for(result <- results.sorted){
      val stationd = result._1
      val temp = result._2
      val formatedTemp = f"$temp%.02f F"
      println(s"$stationd minimun temperature: $formatedTemp")
    }
   }

  def forMaximum(file: RDD[String]) ={
    val rdd = file.map(parseLines)
    val filteredRdd =  rdd.filter(x => x._2 == "TMAX")
    val stationTemps = filteredRdd.map(x => (x._1, x._3.toFloat))
    val minTempByStation = stationTemps.reduceByKey((x,y)=> max(x,y))

    val results = minTempByStation.collect()
    for(result <- results.sorted){
      val stationd = result._1
      val temp = result._2
      val formatedTemp = f"$temp%.02f F"
      println(s"$stationd maximum temperature: $formatedTemp")
    }
  }

  forMinimum(files)
  println("")
  forMaximum(files)
}
