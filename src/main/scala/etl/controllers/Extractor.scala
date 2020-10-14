package etl.controllers

import etl.utils.DataParser
import etl.models.Dataset
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class Extractor(spark: SparkSession) {
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
        "location" -> datasetInfo.get(x).get("location").unwrapped().toString()
      )
      if (configData.getOrElse("type", "") == "csv") {
        val data = Dataset(
          configData.getOrElse("name", ""),
          parser.parseCsv(configData.getOrElse("location", "")))
        results ::= data
      } else {
        val url =
          "http://www.communitybenefitinsight.org/api/get_hospitals.php?"
        val client = HttpClients.createDefault()
        val getFlowInfo: HttpGet = new HttpGet(url)

        val response: CloseableHttpResponse = client.execute(getFlowInfo)
        val entity = response.getEntity
        val jsonString = EntityUtils.toString(entity)
        val mappedData = mapper.readValue[List[Map[String, Any]]](jsonString)
        val data = Dataset("HospitalAPI", parser.parseJson(mapper, mappedData))
        results ::= data
      }
    }

    results
  }
}
