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
    val results = List(Dataset)

    for (dataset <- datasetInfo.toArray()) {
      val configData = Map(
        "name" -> dataset.get("name"),
        "type" -> dataset.get("type"),
        "location" -> dataset.get("location")
      )
      if (configData === "csv") {
        val data =
          Dataset(configData {
            "name"
          }, parser.parseCsv(configData {
            "location"
          }))
        results :+ data
      } else {
        val data = Dataset("Test", parser.parseJson(List("test")))
        results :+ data
      }
    }

    results
  }
}
