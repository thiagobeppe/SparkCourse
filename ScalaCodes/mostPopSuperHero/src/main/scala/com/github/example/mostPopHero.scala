package com.github.example


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext



object mostPopHero extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def countCoOccurrences(line: String) = {
    var elements = line.split("\\s")
    (elements(0).toInt, elements.length-1)
  }

  //Another way to sort by name, transforming on RDD and be available to use in entire cluster
  def parseHeroesName(line: String): Option[(Int, String)] = {
    var fields = line.split("\"")
    if (fields.length > 1) {
      Some(fields(0).trim.toInt, fields(1))
    } else {
      None
    }

  }

  val sc = new SparkContext("local[*]", "mostPopHero")
  val names = sc.textFile("../../datasets/Marvel-names.txt")
  val namesRdd = names.flatMap(parseHeroesName)
  val graphHeroes = sc.textFile("../../datasets/Marvel-graph.txt")
  val pairings = graphHeroes.map(countCoOccurrences)
  val reduced = pairings.reduceByKey((x,y) => x+y).map( x=> (x._2, x._1))
  val findTheMost = namesRdd.lookup(reduced.max._2)(0)
  println(findTheMost)

}
