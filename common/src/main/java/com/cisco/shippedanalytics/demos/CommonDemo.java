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
	public static String root() {
		return "/demos";
	}

	/**
	 * FileSystem
	 */
	public static FileSystem fs(String demo) throws IllegalArgumentException, IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(root()), new Configuration());

		// making a clean directory
		fs.mkdirs(new Path(root() + demo));
		fs.delete(new Path(root() + demo), true);
		fs.mkdirs(new Path(root() + demo));
		return fs;
	}

	/**
	 * Report success to HDFS and exit
	 */
	public static void succeed(String path) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path(root() + path));

		FSDataOutputStream os = fs.create(new Path(root() + path + "/SUCCESS"));
		IOUtils.write("Succeeded on " + new Date(), os);
		os.close();

		System.exit(0);
	}

	/**
	 * Report failure to HDFS and exit
	 */
	public static void fail(String path, String message) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path(root() + path));

		FSDataOutputStream os = fs.create(new Path(root() + path + "/FAILUE"));
		IOUtils.write("FAILED on " + new Date() + " with message\n" + message, os);
		os.close();

		System.exit(1);
	}

}

