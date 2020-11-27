package etl.controllers

import etl.utils.DataParser
import etl.models.Dataset
import etl.utils.LogHelper
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class Extractor(spark: SparkSession) extends LogHelper {
  def extract(): List[Dataset] = {
    val parser = new DataParser(spark)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val config = ConfigFactory.load("application.conf")
    val datasetInfo = config.getObjectList("datasetInfo")
    val datasetsLength = datasetInfo.size()
    var results = List[Dataset]()

    for (x <- 0 to datasetsLength - 1) {
      val configData = Map(
        "name" -> datasetInfo.get(x).get("name").unwrapped().toString(),
        "type" -> datasetInfo.get(x).get("type").unwrapped().toString(),
        "location" -> datasetInfo.get(x).get("location").unwrapped().toString(),
        "joinColumn" -> datasetInfo
          .get(x)
          .get("joinColumn")
          .unwrapped()
          .toString(),
        "transformations" -> datasetInfo
          .get(x)
          .get("transformations")
          .unwrapped()
          .toString()
      )
      val datasetType = configData.getOrElse("type", "")
      datasetType match {
        case "csv" => {
          val data = Dataset(
            configData.getOrElse("name", ""),
            configData.getOrElse("joinColumn", ""),
            parser.parseCsv(configData.getOrElse("location", "")),
            configData.getOrElse("transformations", "").split(",")
          )
          results ::= data
        }
        case "api" => {
          val url = configData.getOrElse("location", "")
          val client = HttpClients.createDefault()
          val getFlowInfo: HttpGet = new HttpGet(url)

          val response: CloseableHttpResponse = client.execute(getFlowInfo)
          val entity = response.getEntity
          val jsonString = EntityUtils.toString(entity)
          val mappedData = mapper.readValue[List[Map[String, Any]]](jsonString)
          val data = Dataset(
            configData.getOrElse("name", ""),
            configData.getOrElse("joinColumn", ""),
            parser.parseJson(mapper, mappedData),
            configData.getOrElse("transformations", "").split(",")
          )
          results ::= data
        }
        case _ => logger.error("Invalid dataset type provided")
      }
    }
    results
  }
}
