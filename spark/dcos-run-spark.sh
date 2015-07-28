# Need to copy spark-demo-with-dependencies.jar to 128.107.8.171/usr/share/spark/
dcos spark run --submit-args='--class com.cisco.shippedanalytics.demos.spark.Run http://128.107.8.171/spark-demo-with-dependencies.jar'
# /opt/mesosphere/packages/hadoop--a0b14ad939d0ed373688435552ff63f724608a78/bin/hdfs dfs -getmerge /demos/spark/shakespeare shakespeare.txt
 
