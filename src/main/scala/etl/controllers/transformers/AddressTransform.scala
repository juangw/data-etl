package etl.controllers.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

class AddressTransform {
  private val stripFormatting: String => String = _.toLowerCase().replaceAll("[,.]", "")
  private val stripFormattingUDF = udf(stripFormatting)

  def transform(data: DataFrame, column: String): DataFrame = {
    data.withColumn(column, stripFormattingUDF(data("%s".format(column))))
  }
}
