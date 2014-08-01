# Helen

[![Build Status](https://travis-ci.org/vptheron/helen.svg?branch=master)](https://travis-ci.org/vptheron/helen)

A fully asynchronous driver for [Apache Cassandra](http://cassandra.apache.org/) written in Scala and built on top of [Akka IO](http://akka.io/).

This driver is built and tested with Cassandra 2.0.9 and uses version 2 of CQL3.

## Low level API

For now, the only available API. It provides an easy way to create a connection to a Cassandra node and to send/receive requests/responses.

### Connecting to a node

First, you will need an `ActorSystem` in scope:

```scala
val system = ActorSystem("helen-system")
```

Then, you can either deal with the `ConnectionActor` directly:

```scala
val actor: ActorRef = system.actorOf(ConnectionActor.props("localhost", 9042))
```

or instantiate an `ActorBackedCqlClient` (which will internally instantiate a `ConnectionActor`):

```scala
val client: CqlClient = new ActorBackedCqlClient("localhost", 9042)(system)
```

The `CqlClient` provides you with some type safety since it's `send` method only accepts `Request` instances and returns `Future[Responses]`.

**One actor = one connection**: one `ConnectionActor` = one connection to one node, there is no support for reconnection or load balancing between nodes of the same cluster.

### Sending requests

The `Requests` object contains all the available request frames supported by the CQL protocol. `Requests.Startup` should be the very first request you sent.

```scala
actor ! Requests.Startup
actor ! Requests.Query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}")
client.send(Requests.Query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)"))
client.send(Requests.Query("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'})"))
```

Other types of request are available like `Prepare`, `Execute`, `Batch`, etc.

**Global Keyspace**: Since one actor is a wrapper for one connection, you can safely issue a `USE KEYSPACE` request to set a current keyspace for that connection.

### Parameterized statements

Starting with Cassandra 2.0, both normal and prepared statements support ? markers. The `Values` object contains helper functions to serialize/deserialize scala types to/from CQL representation.

```scala
val prepared = client.send(Requests.Prepare("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")).asInstanceOf[Prepared]

val boundValues = List(
  Values.uuidToBytes(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")),
  Values.textToBytes("La Petite Tonkinoise"),
  Values.textToBytes("Bye Bye Blackbird"),
  Values.textToBytes("Josephine Baker"),
  Values.setToBytes(Set("jazz", "2014"), Values.textToBytes)
)

client.send(Requests.Execute(prepared.id, QueryParameters(values = boundValues)))
```
