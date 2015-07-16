# Cassandra Sample Reader/Writer App

Get a directory with 
```
cassandra-demo-with-dependencies.jar
run-cassandra.sh
```

From this directory run 

```
sh run-cassandra.sh
```

And wait for the word 'SUCCESS'.

You can also explore the Cassandra table 
```
demo.load_averages
```
 
Built with [Scala](https://github.com/scala/scala) and [DataStax Cassandra driver ](https://github.com/datastax/java-driver).

Reads */proc/loadavg* file and inserts load average data into Cassandra's *demo.load_averages* table.

