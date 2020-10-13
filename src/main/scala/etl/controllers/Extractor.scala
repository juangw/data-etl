package etl.controllers

import etl.utils.DataParser
import etl.models.Dataset
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

class Extractor(spark: SparkSession) {
  def extract(): List[Dataset] = {
    val parser = new DataParser(spark)
    val config = ConfigFactory.load("application.conf")
    val datasetInfo = config.getObjectList("datasetInfo")
    val datasetsLength = datasetInfo.size()
    var results = List[Dataset]()

    for (x <- 0 to datasetsLength - 1) {
      val configData = Map(
        "name" -> datasetInfo.get(x).get("name").unwrapped().toString(),
        "type" -> datasetInfo.get(x).get("type").unwrapped().toString(),
        "location" -> datasetInfo.get(x).get("location").unwrapped().toString()
      )
      if (configData.getOrElse("type", "") == "csv") {
        val data = Dataset(
          configData.getOrElse("name", ""),
          parser.parseCsv(configData.getOrElse("location", "")))
        results ::= data
      } else {
        val data = Dataset("Test", parser.parseJson(List("test")))
        results ::= data
      }
    }

    results
  }
}
