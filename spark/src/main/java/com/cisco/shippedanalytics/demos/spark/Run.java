package com.cisco.shippedanalytics.demos.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.cisco.shippedanalytics.demos.CommonDemo;

/**
 * Basic demo showing that Spark is installed and running.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String DEMO = "/spark";

	private static final String SHAKESPEARE_TXT = "/shakespeare.txt";

	private static final String APP_NAME = "Shipped Analytics - Spark Demo";

	public static void main(String[] args) throws IOException, IllegalArgumentException, URISyntaxException {

		if (args.length != 1) {
			System.err.println("Expected an argument - HDFS ip");
		}

		String hdfs = args[0];

		System.out.println("Using HDFS " + hdfs);

		FileSystem fs = CommonDemo.fs(hdfs, DEMO);
		fs.delete(new Path(CommonDemo.root(hdfs) + DEMO), true);

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);

		try (JavaSparkContext context = new JavaSparkContext(sparkConf)) {

			List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream(SHAKESPEARE_TXT));
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

			JavaRDD<String> wordLines = wordCounts.map(new Function<Tuple2<String,Integer>, String>() {

				// generated UID
				private static final long serialVersionUID = -3808131749775009311L;

				@Override
				public String call(Tuple2<String, Integer> tuple) throws Exception {
					return tuple._1() + "-" + tuple._2();
				}
			});

			FSDataOutputStream os = fs.create(new Path(CommonDemo.root(hdfs) + DEMO + SHAKESPEARE_TXT));
			IOUtils.writeLines(wordLines.collect(), "\n", os);
			os.close();

		}

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(CommonDemo.root(hdfs) + DEMO + SHAKESPEARE_TXT))));
		for (String line : IOUtils.readLines(br)) {
			if (line.contains("COUNTESS")) {
				if (!line.contains("43")) {
					CommonDemo.fail(hdfs, DEMO, "Expected 43 occurances of COUNTESS, but did not found in the following line: " + line);
				}
			}
		}

		CommonDemo.succeed(hdfs, DEMO);
	}

}

