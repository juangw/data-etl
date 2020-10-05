package etl

import org.apache.spark.sql.SparkSession
import etl.utils.CsvParser

object Application extends App {
  var reader = new CsvParser()
  var results = reader.readCsv()

  val spark = SparkSession.builder().getOrCreate()
  println(results)
  println("hello world")
}