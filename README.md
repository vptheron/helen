# Helen

[![Build Status](https://travis-ci.org/vptheron/helen.svg?branch=master)](https://travis-ci.org/vptheron/helen)

A fully asynchronous driver for [Apache Cassandra](http://cassandra.apache.org/) written in Scala and built on top of [Akka IO](http://akka.io/).

This driver is built and tested with Cassandra 2.1.0 and uses version 3 of the CQL binary protocol.

This project is mostly used to experiment with Akka-IO and diverse interesting libraries. It is *not* to be used since the API will most likely change dramatically.

## Low level API

For now, the only available API. It provides an easy way to create a connection to a Cassandra node and to send/receive requests/responses.

### Connecting to a node

First, you will need an `ActorSystem` in scope:

```scala
val system = ActorSystem("helen-system")
```

Then you can instantiate an `ActorBackedCqlClient`:

```scala
val client: CqlClient = new ActorBackedCqlClient("localhost", 9042)(system)
```

The `CqlClient` above will open 1 connection to `localhost:9042`. The first message to start a CQL session (`Startup`) is automatically sent.

### Sending requests

The `Requests` object contains all the available request frames supported by the CQL protocol. You do not need to send `Requests.Startup`.

```scala
client.send(Requests.Query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)"))
client.send(Requests.Query("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'})"))
```

Other types of request are available like `Prepare`, `Execute`, `Batch`, etc.

**Global Keyspace**: the `CqlClient` internally maintains one single connection to the Cassandra node, you can issue a `use keyspace` query to set the context of this connection.

### Parameterized statements

Starting with Cassandra 2.0, both normal and prepared statements support ? markers. The `Codecs` object contains helper functions as well as implicit imports to serialize/deserialize scala types to/from CQL representation.

```scala
import io.helen.cql.Codecs.Implicits._

val prepared = client.send(Requests.Prepare("INSERT INTO demodb.songs (id, title, album, artist, datetime) VALUES (?, ?, ?, ?, ?)")).asInstanceOf[Prepared]

val boundValues = List(
  UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50").asUUID,
  "La Petite Tonkinoise".asText,
  "Bye Bye Blackbird".asAscii,
  "Josephine Baker".asVarchar,
  DateTime.now.asTimestamp
)

client.send(Requests.Execute(prepared.id, QueryParameters(values = boundValues)))
```
