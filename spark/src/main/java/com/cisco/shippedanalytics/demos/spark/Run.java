package com.cisco.shippedanalytics.demos.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Basic demo showing that Spark is installed and running.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String HDFS_OUTPUT = "/demos/spark/shakespeare";

	private static final Logger logger = LoggerFactory.getLogger(Run.class);

	private static final String APP_NAME = "Shipped Analytics - Spark Demo";

	public static void main(String[] args) throws IOException {

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);

		try (JavaSparkContext context = new JavaSparkContext(sparkConf)) {

			List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream("/shakespeare.txt"));
			JavaRDD<String> file = context.parallelize(text);

			JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {

				// generated UID
				private static final long serialVersionUID = -1341983880061131332L;

				@Override
				public Iterable<String> call(String s) {
					return Arrays.asList(s.split("\\W+"));
				}
			});
			JavaPairRDD<String, Integer> wordCountPairs = words.mapToPair(new PairFunction<String, String, Integer>() {

				// generated UID
				private static final long serialVersionUID = 955624374538945598L;

				@Override
				public Tuple2<String, Integer> call(String s) {
					return new Tuple2<String, Integer>(s, 1);
				}
			});

			JavaPairRDD<String, Integer> wordCounts = wordCountPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

				// generated UID
				private static final long serialVersionUID = -5092320117156765986L;

				@Override
				public Integer call(Integer a, Integer b) {
					return a + b;
				}
			});

			wordCounts.saveAsTextFile(HDFS_OUTPUT);
			logger.info(APP_NAME + " completed successfully with the first Act of All's Well That Ends Well saved to HDFS to " + HDFS_OUTPUT + wordCounts.count());


			FileSystem fs = FileSystem.get(new Configuration());
			//			fs.mkdirs(new Path(StringUtils.substringBeforeLast(HDFS_OUTPUT, "/")));

			try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(HDFS_OUTPUT))))) {
				for (String line : IOUtils.readLines(br)) {
					if (line.contains("COUNTESS")) {
						if (!line.contains("43")) {
							logger.error("Expected 43 occurances of COUNTESS, but did not found in the following line: " + line);
							System.exit(1);
						}
					}
				}
			}
		}
	}

}

