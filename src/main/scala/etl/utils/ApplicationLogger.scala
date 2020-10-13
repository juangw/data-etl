package etl.utils

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext

class ApplicationLogger(sparkContext: SparkContext) {
  def initLogger() = {
    sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }
}
