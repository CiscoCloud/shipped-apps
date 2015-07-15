# Cassandra Sample Reader/Writer App

Built with [Scala](https://github.com/scala/scala) and awesome [DataStax Cassandra driver ](https://github.com/datastax/java-driver).

Reads */proc/loadavg* file and inserts load average data into Cassandra's *sampleapp.load_averages* table.

To build please issue:

```bash
mvn clean package
```

To use the app in writer mode:
```bash
cqlsh -f CQL/createTable.cql cassandra_node_ip
java -jar target/cassandra-demo-with-dependencies.jar \
-n cassandra_node_ip \
-m w \
-i data_fetching_interval \
-p cassandra_port
```

In reader mode:
```bash
java -jar target/cassandra-demo-with-dependencies.jar \
-n cassandra_node_ip \
-m r \
-p cassandra_port
```

To see more verbose output add "-d true" argument in any mode.

To see usage information run the jar without arguments.

