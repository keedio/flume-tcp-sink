# Flume TCP Sink

This flume sink is intended to send the events over a TCP connection.

Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy flume-tcp-sink-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/tcp-sink/lib
  $ cp flume-ng-sql-source-0.0.3.jar $FLUME_HOME/plugins.d/sql-source/lib
```

Configuration
----------

| Property name | Default value | Description
| ----------------------- | :-----: | :---------- |
| <b>hostname</b> | - | The hostname or IP address to connect to
| <b>port</b> | - | 	The port number of remote host to connect
| batchSize | 100 | Number of events to be written per txn
| connectionTimeout | 10 | Seconds to wait to stablish the connection
| connectionRetries | 0 | How much retries to open the TCP connectionthe when some error happens (0 means no limit)
| connectionRetryDelay | 10 | Seconds to wait before new connection retry

Configuration example
---------------------
```
a1.channels = c1
a1.sinks = k1
a1.sinks.k1.type = org.keedio.flume.sink.TcpSink
a1.sinks.k1.hostname = 127.0.0.1
a1.sinks.k1.port = 5140 
```
