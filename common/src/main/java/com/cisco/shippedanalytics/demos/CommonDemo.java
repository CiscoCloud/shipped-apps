package com.cisco.shippedanalytics.demos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Common code.
 *
 * @author Borys Omelayenko
 *
 */
public class CommonDemo {

	/**
	 * Demo HDFS root
	 */
	public static String root(String hdfs) {
		return hdfs + "/demos";
	}

	/**
	 * FileSystem
	 */
	public static FileSystem fs(String hdfs, String demo) throws IllegalArgumentException, IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(root(hdfs)), new Configuration());

		// making a clean directory
		fs.mkdirs(new Path(root(hdfs) + demo));
		fs.delete(new Path(root(hdfs) + demo), true);
		fs.mkdirs(new Path(root(hdfs) + demo));
		return fs;
	}

	/**
	 * Report success to HDFS and exit
	 */
	public static void succeed(String hdfs, String path) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path(root(hdfs) + path));

		FSDataOutputStream os = fs.create(new Path(root(hdfs) + path + "/SUCCESS"));
		IOUtils.write("Succeeded on " + new Date(), os);
		os.close();

		System.exit(0);
	}

	/**
	 * Report failure to HDFS and exit
	 */
	public static void fail(String hdfs, String path, String message) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path(root(hdfs) + path));

		FSDataOutputStream os = fs.create(new Path(root(hdfs) + path + "/FAILURE"));
		IOUtils.write("FAILED on " + new Date() + " with message\n" + message, os);
		os.close();

		System.exit(1);
	}

}

