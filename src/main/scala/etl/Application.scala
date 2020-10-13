package etl

import etl.utils.ApplicationLogger
import etl.controllers.Extractor

import org.apache.spark.sql.SparkSession

object Application extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val logger = new ApplicationLogger(sc).initLogger()

  val extractor = new Extractor(spark)
  var results = extractor.extract()

  for (result <- results) {
    println(result)
  }
}
