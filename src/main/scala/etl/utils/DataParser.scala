package etl.utils

import org.apache.spark.sql.{SparkSession, DataFrame}

class DataParser(spark: SparkSession) {
  def parseCsv(fileName: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
  }

  def parseJson(jsonString: List[String]): DataFrame = {
    val data = spark.sparkContext.parallelize(jsonString)
    spark.read.json(data)
  }
}
