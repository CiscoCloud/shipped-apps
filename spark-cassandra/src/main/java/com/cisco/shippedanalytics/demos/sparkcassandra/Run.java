package com.cisco.shippedanalytics.demos.sparkcassandra;

import java.io.IOException;
import java.net.URISyntaxException;
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

import com.cisco.shippedanalytics.demos.CommonDemo;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

/**
 * Basic demo showing that Spark is installed and running.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String TABLE = "logrecords";

	private static final String KEYSPACE = "demos";

	private static final String DEMO = "/spark-cassandra";

	private static final String ACCESS_LOG = "/access.log";

	private static final String APP_NAME = "Shipped Analytics - Spark-Cassandra Demo";

	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length != 2) {
			CommonDemo.fail("", DEMO, "Expected two parameters - cassandra-ip hdfs-ip");
		}
		String cassandraHost = args[0];
		String hdfs = args[1];

		// copy log file to HDFS
		List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream(ACCESS_LOG));
		FileSystem fs = CommonDemo.fs(hdfs, DEMO);

		FSDataOutputStream os = fs.create(new Path(CommonDemo.root(hdfs) + DEMO + ACCESS_LOG));
		IOUtils.writeLines(text, "\n", os);
		os.close();

		// read log file with Spark
		SparkConf sparkConf = new SparkConf()
		.setAppName(APP_NAME)
		.set("spark.cassandra.keyspace", KEYSPACE)
		.set("spark.cassandra.connection.host", cassandraHost);

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<LogRecord> records = null;
		try {
			records = sparkContext.textFile(CommonDemo.root(hdfs) + DEMO + ACCESS_LOG).map(new Function<String, LogRecord>() {

				private static final long serialVersionUID = -4197368391364369559L;

				@Override
				public LogRecord call(String line) throws Exception {
					String[] fields = StringUtils.split(line, ' ');
					return new LogRecord(fields[0], fields[1], Integer.parseInt(fields[2]), fields[3]);
				}
			});
		} catch (Exception e) {
			CommonDemo.fail(hdfs, DEMO, "Exception while reading log data from " + (CommonDemo.root(hdfs) + DEMO + ACCESS_LOG) + "\n" + e.getMessage());
		}

		System.out.println("Phase 1");

		// save log records to Cassandra
		try {
			CassandraConnector connector = CassandraConnector.apply(sparkContext.getConf());

			Session session = connector.openSession();
			session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
			session.execute("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE + " (ip text, time int, url text, user text, PRIMARY KEY (ip, time))");
			session.close();
		} catch (Exception e) {
			CommonDemo.fail(hdfs, DEMO, "Exception while creating Cassandra tables\n" + e.getMessage());
		}

		System.out.println("Phase 2");

		try {
			//CassandraJavaUtil.javaFunctions(records).writerBuilder(KEYSPACE, TABLE, CassandraJavaUtil.mapToRow(LogRecord.class)).saveToCassandra();
		} catch (Exception e) {
			CommonDemo.fail(hdfs, DEMO, "Exception while writing data to Cassandra\n" + e.getMessage());
		}

		System.out.println("Phase 3");

		// read log records from Cassandra and check
		try {
			JavaRDD<LogRecord> read = CassandraJavaUtil.javaFunctions(sparkContext).cassandraTable(KEYSPACE, TABLE, CassandraJavaUtil.mapRowTo(LogRecord.class));
			if (read.count() != text.size()) {
				CommonDemo.fail(hdfs, CommonDemo.root(hdfs) + DEMO, "Expected to read " + text.size() + " records but found " + read.count());
			}
		} catch (Exception e) {
			CommonDemo.fail(hdfs, DEMO, "Exception while reading data from Cassandra\n" + e.getMessage());
		}
		CommonDemo.succeed(hdfs, DEMO);
	}

}
