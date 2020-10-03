package main.scala

import main.scala.utils.{CsvReader}
import org.apache.spark.sql.SparkSession


object Application extends App {
  var reader = new CsvReader()
  var results = reader.readCsv()
  println(results)
  println("hello world")
}
