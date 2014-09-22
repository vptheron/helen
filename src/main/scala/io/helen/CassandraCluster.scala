package io.helen

trait CassandraCluster {

  def newSession(keyspace: Option[String] = None): CassandraSession
}
