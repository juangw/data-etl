package etl.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

trait LogHelper {
  val loggerName: String = this.getClass.getName
  lazy val logger: Logger = Logger.getLogger(loggerName)
  Logger.getRootLogger.setLevel(Level.INFO)
}

class ApplicationLogger() {
  def initLogger() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def initSparkLogger(sparkContext: SparkContext) = {
    sparkContext.setLogLevel("ERROR")
  }
}
