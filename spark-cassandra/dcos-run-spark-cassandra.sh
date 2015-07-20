# Need to copy spark-cassandra-demo-with-dependencies.jar to 128.107.8.171/usr/share/spark/
dcos spark run --submit-args='--class com.cisco.shippedanalytics.demos.spark.Run http://128.107.8.171/spark-cassandra-demo-with-dependencies.jar' 
