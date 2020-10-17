package etl

import etl.utils.ApplicationLogger
import etl.controllers.{Extractor, Transformer}

import org.apache.spark.sql.SparkSession

object Application extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val logger = new ApplicationLogger(sc).initLogger()

  val extractor = new Extractor(spark)
  var datasets = extractor.extract()

  val transformer = new Transformer(datasets)
  var transformedDatasets = transformer.transform()
}
