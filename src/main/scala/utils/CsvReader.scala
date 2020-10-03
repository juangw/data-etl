package main.scala.utils

import scala.io.{Source}

class CsvReader {
  def readCsv(): Unit = {
    val bufferedSource = Source.fromFile("./data/ACSDP1Y2010.DP04_data_with_overlays_2020-10-02T231835.csv")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      // do whatever you want with the columns here
      println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
    }
    bufferedSource.close
  }
}
