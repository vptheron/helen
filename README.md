# Helen

A Scala client to Cassandra.

This is a very minimalistic project. The source code is very small since, as of now, it is merely a thin wrapper around the Datastax Java driver.

## Connecting to the database

You need a `Cluster` instance. It is as simple as providing a list of the nodes in the Cassandra cluster:

    val cluster = new Cluster( List("127.0.0.1", "192.168.0.3") )

You can use other parameters to override the port and the compression mode (default are 9042 and None).

Once you hold a `Cluster` instance you can connect to the database and create a `Session`:

    val sessionWithoutKeySpace = cluster.newSession()
    val sessionAttachedToAKeySpace = cluster.newSession(Some("myKeySpace"))

A `Session` is thread-safe so it is enough to create one instance per key space for the whole application. However, it is impossible to reassign a `Session` to a different key space.

## Querying the database

You can use a `Session` instance to issue queries against the database.

    val f: Future[List[Row]] = session.execute("SELECT * FROM myTable WHERE id = 42;")

Executing a query returns a Scala `Future` wrapping a `List` of `Row`s - your result set.
