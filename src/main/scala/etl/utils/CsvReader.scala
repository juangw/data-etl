package etl.utils

import org.apache.spark.sql.{SparkSession, DataFrame}

class DataReader(spark: SparkSession) {
  def readCsv(fileName: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema",  true)
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
  }
}
