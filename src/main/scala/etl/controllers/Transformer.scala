package etl.controllers

import etl.models.Dataset
import etl.controllers.transformers.AddressTransform

class Transformer(datasets: List[Dataset]) {
  def transform(): List[Dataset] = {
    for (dataset <- datasets) {

      // Do transformations to data
      for (transformation <- dataset.transformations) {
        val transform = transformation.split('|')
        if (transform.length == 2) {
          transform{1} match {
            case "AddressTransform" => {
              val transformer = new AddressTransform()
              val transformedData = transformer.transform(dataset.dataset, transform{0})
              println(transformedData.select(transform{0}).show(10))
            }
          }
        }
      }
    }

    datasets
  }
}
