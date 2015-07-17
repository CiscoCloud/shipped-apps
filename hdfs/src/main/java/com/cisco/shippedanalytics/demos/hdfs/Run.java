package com.cisco.shippedanalytics.demos.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Basic demo for HDFS read-write.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String HDFS_OUTPUT = "/demos/hdfs/shakespeare.txt";

	public static void main(String[] args) throws IOException, URISyntaxException {

		List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream("/shakespeare.txt"));
		Configuration hadoopConfig = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_OUTPUT), hadoopConfig);
		fs.mkdirs(new Path(StringUtils.substringBeforeLast(HDFS_OUTPUT, "/")));

		FSDataOutputStream os = fs.create(new Path(HDFS_OUTPUT));
		IOUtils.writeLines(text, "\n", os);
		os.close();

		FSDataInputStream is = fs.open(new Path(HDFS_OUTPUT));
		List<String> read = IOUtils.readLines(is);
		is.close();

		if (text.size() != read.size()) {
			System.err.println("Error: expected " + text.size() + " lines but read " + read.size());
			System.exit(1);
		}
		for (int i = 0; i < read.size(); i ++) {
			if (!read.get(i).equals(text.get(i))) {
				System.err.println("Error: expected '" + text.get(i) + "' but read '" + read.get(i) + "' on line " + i);
				System.exit(1);
			}
		}
		System.out.println("SUCCESS, created file " + HDFS_OUTPUT);
	}

}

