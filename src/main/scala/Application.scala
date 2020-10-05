package main.scala

import main.scala.utils.{CsvReader}
import org.apache.spark.sql.SparkSession


object Application extends App {
  var reader = new CsvReader()
  var results = reader.readCsv()

  val spark = SparkSession.builder().getOrCreate()
  println(results)
  println("hello world")
}
