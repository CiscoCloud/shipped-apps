hdfs dfs -rm -r -f -skipTrash /user/$USER/sasa-spark/output

# -i /user/bomelaye/sasa-logs/20150224/*/*/*/* \
#  -i /user/bomelaye/sasa-logs/instance-01/80/parsed_access_referer_log.20150224-2* \
spark-submit --class com.cisco.sasa.runner.Convert --master spark://gmo-cl-edge-02:7077 --driver-class-path /home/bomelaye/.m2/repository/org/apache/hadoop/hadoop-common/2.5.0/hadoop-common-2.5.0.jar;/home/bomelaye/spark/sasa-dependencies.jar \
 /home/$USER/spark/sasa.jar \
 -i /user/bomelaye/sasa-logs/20150224/www.cisco.com/*/*/* \
 -o /user/$USER/sasa-spark/output \
 -c 180 \
 -d 2015-2-5 \
 -a 173.39.214.110
 
hdfs dfs -get /user/$USER/sasa-spark/output
 
