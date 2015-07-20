package com.cisco.shippedanalytics.demos.sparkcassandra;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cisco.shippedanalytics.demos.CommonDemo;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

/**
 * Basic demo showing that Spark is installed and running.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String TABLE = "log-records";

	private static final String KEYSPACE = "demos";

	private static final String CASSANDRA = "/spark-cassandra";

	private static final String ACCESS_LOG = "/access.log";

	private static final String APP_NAME = "Shipped Analytics - Spark Demo";

	public static void main(String[] args) throws IOException, URISyntaxException {

		// copy log file to HDFS
		List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream(ACCESS_LOG));
		FileSystem fs = CommonDemo.fs(CASSANDRA);

		FSDataOutputStream os = fs.create(new Path(CommonDemo.root() + CASSANDRA + ACCESS_LOG));
		IOUtils.writeLines(text, "\n", os);
		os.close();

		// read log file with Spark
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<LogRecord> records = sparkContext.textFile(CommonDemo.root() + CASSANDRA + ACCESS_LOG).map(new Function<String, LogRecord>() {

			private static final long serialVersionUID = -4197368391364369559L;

			@Override
			public LogRecord call(String line) throws Exception {
				String[] fields = StringUtils.split(line, ' ');
				return new LogRecord(fields[0], fields[1], new Date(), fields[3]);
			}
		});

		// save log records to Cassandra
		// FIXME: uniqueness of records and table creation
		CassandraJavaUtil.javaFunctions(records).writerBuilder(KEYSPACE, TABLE, CassandraJavaUtil.mapToRow(LogRecord.class)).saveToCassandra();

		// read log records from Cassandra and check
		JavaRDD<LogRecord> read = CassandraJavaUtil.javaFunctions(sparkContext).cassandraTable(KEYSPACE, TABLE, CassandraJavaUtil.mapRowTo(LogRecord.class));
		if (read.count() != text.size()) {
			CommonDemo.fail(CommonDemo.root() + CASSANDRA + ACCESS_LOG, "Expected to read " + text.size() + " records but found " + read.count());
		}
		CommonDemo.succeed(CommonDemo.root() + CASSANDRA + ACCESS_LOG);
	}

}
