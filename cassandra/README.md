# how to run

* build it using `mvn clean install`, after the build is complete you only need file `target\cassandra-demo-with-dependencies.jar` that you can copy to the target system.

* modify as needed and execute `sh run-cassandra.sh` 

# how to assess

Give it a couple of minutes to run. You may monitor its progress in MESOS.

* check HDFS directory `hadoop dfs -ls /demos/cassandra`

* It should contain file `SUCCESS` or `FAILURE` depending on the result.

* `SUCCESS` indicates a successful completion of the demo

* `FAILURE` describes the reason

# additional info

The demo creates the Cassandra table `demo.load_averages` that can be explored after the script completes.


