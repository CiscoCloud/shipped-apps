# shipped-apps
Demo and sample applications shipped with the ShippedAnalytics cluster

It feature the following demos (implemented as maven modules)

* `hdfs` that check that HDFS is running and attempts to write a file and read it. It runs on the name node

* `spark` reads a file from HDFS, performs word count, and writes it back to HDFS. 

* `cassandra` is similar to HDFS but writes/reads from a Cassandra database

* `spark-cassandra` reads/writes data via HDFS and Spark-Cassandra connector, testing all these three components together

# how to build

Use Apache Maven, run command `mvn install` from the directory of this readme file.



 
