# Need to copy spark-cassandra-demo-with-dependencies.jar to 128.107.8.171/usr/share/spark/
dcos spark run --submit-args='--class com.cisco.shippedanalytics.demos.sparkcassandra.Run http://128.107.8.171/spark-cassandra-demo-with-dependencies.jar 173.37.40.244' 

#cd /var/lib/mesos/slave/slaves/20150701-162235-3978831277-5050-13214-S1/frameworks/20150701-162235-3978831277-5050-13214-0004/executors/executor.datanode.NodeExecutor.1437054557927/runs/latest/