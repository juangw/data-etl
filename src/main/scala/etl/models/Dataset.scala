package etl.models

import org.apache.spark.sql.DataFrame

case class Dataset(datasetName: String,
                   joinColumn: String,
                   dataset: DataFrame,
                   transformations: Array[String])
