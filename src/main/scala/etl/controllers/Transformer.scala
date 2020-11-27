package etl.controllers

import etl.models.Dataset
import etl.controllers.transformers.AddressTransform

class Transformer(datasets: List[Dataset]) {
  def transform(): List[Dataset] = {
    datasets.map(
      dataset =>
        dataset.transformations.map(
          transformation => {
            val transform = transformation.split('|')
            if (transform.length == 2) {
              transform { 1 } match {
                case "AddressTransform" => {
                  val transformer = new AddressTransform()
                  transformer.transform(dataset.dataset, transform { 0 })
                }
              }
            }
          }
      )
    )
    datasets
  }
}
