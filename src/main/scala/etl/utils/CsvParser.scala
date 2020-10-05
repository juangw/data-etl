package etl.utils

import scala.io.Source

class CsvParser {
  def readCsv(): Unit = {
    val bufferedSource = Source.fromFile("./data/Definitive_Healthcare%3A_USA_Hospital_Beds.csv")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      // do whatever you want with the columns here
      println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
    }
    bufferedSource.close
  }
}
