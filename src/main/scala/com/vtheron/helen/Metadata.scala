package com.vtheron.helen

import com.datastax.driver.core.{Metadata => JMetadata}

class Metadata private[helen](metadata: JMetadata) {

  def clusterName: String = metadata.getClusterName

}
