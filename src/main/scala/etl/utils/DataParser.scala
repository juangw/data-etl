package etl.utils

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.fasterxml.jackson.databind.ObjectMapper

class DataParser(spark: SparkSession) {
  def parseCsv(fileName: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
  }

  def parseJson(mapper: ObjectMapper, dataList: List[Map[String, Any]]): DataFrame = {
    var jsonList = List[String]()
    for (data <- dataList) {
      val jsonResult = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(data)
      jsonList ::= jsonResult
    }
    val data = spark.sparkContext.parallelize(jsonList)
    spark.read.json(data)
  }
}
