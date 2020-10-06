package etl

import etl.utils.DataReader
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Application extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }
  init

  var reader = new DataReader(spark)
  var dataFrame =
    reader.readCsv("./data/Definitive_Healthcare%3A_USA_Hospital_Beds.csv")

  dataFrame.show(2)
}
