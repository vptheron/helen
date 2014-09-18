package io.helen.main

import com.datastax.driver.core.Cluster

object DatastaxMain {

  def main(args: Array[String]){
  
    val cluster = Cluster.builder().addContactPoint("localhost").build()

    val clusterMetadata = cluster.getMetadata
    clusterMetadata.getAllHosts
    clusterMetadata.getClusterName
    clusterMetadata.getKeyspaces

    val session = cluster.connect("myKeyspace")

    val resultSet = session.execute("SELECT blabla")
    val row = resultSet.one()
    row.getLong(42)
    row.getBool("aBoolColumn")
    row.getString(7)

  }
  
}
