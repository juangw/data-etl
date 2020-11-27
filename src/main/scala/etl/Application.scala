package etl

import etl.utils.{ApplicationLogger, LogHelper}
import etl.controllers.{Extractor, Transformer}

import org.apache.spark.sql.SparkSession

object Application extends App with LogHelper {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  new ApplicationLogger().initLogger()
  new ApplicationLogger().initSparkLogger(sc)

  val extractor = new Extractor(spark)
  var datasets = extractor.extract()
  logger.info(f"Extracted ${datasets.size} number of datasets")

  val transformer = new Transformer(datasets)
  var transformedDatasets = transformer.transform()
  logger.info(f"Transformed ${transformedDatasets.size} number of datasets")
}
