# Need to copy spark-demo-with-dependencies.jar to 128.107.8.171/usr/share/spark/
dcos spark run --submit-args='--class com.cisco.shippedanalytics.demos.spark.App --master spark://173.37.40.240:10001 http://128.107.8.171/spark-demo-with-dependencies.jar' 
