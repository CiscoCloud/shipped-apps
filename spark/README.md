# how to run

* build it using `mvn clean install`, after the build is complete you only need file `target\spark-demo-with-dependencies.jar` that you can copy to the target system.

* deploy it to a location where it can be accessed by an URL (see the example in the `dcos-run-spark.sh` script)  

* modify as needed and execute `sh dcos-run-spark.sh` 

# how to assess

Give it a couple of minutes to run. You may monitor its progress in MESOS or SPARK.

* check HDFS directory `hadoop dfs -ls /demos/spark`

* It should contain file `SUCCESS` or `FAILURE` depending on the result.

* `SUCCESS` indicates a successful completion of the demo

* `FAILURE` describes the reason



