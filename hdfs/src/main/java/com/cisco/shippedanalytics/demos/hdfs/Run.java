package com.cisco.shippedanalytics.demos.hdfs;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cisco.shippedanalytics.demos.CommonDemo;

/**
 * Basic demo for HDFS read-write.
 *
 * @author Borys Omelayenko
 *
 */
public class Run {

	private static final String DEMO = "/hdfs";
	private static final String SHAKESPEARE_TXT = "/shakespeare.txt";

	public static void main(String[] args) throws IOException, URISyntaxException {

		List<String> text = IOUtils.readLines(new Run().getClass().getResourceAsStream(SHAKESPEARE_TXT));
		FileSystem fs = CommonDemo.fs(DEMO);

		FSDataOutputStream os = fs.create(new Path(CommonDemo.root() + DEMO + SHAKESPEARE_TXT));
		IOUtils.writeLines(text, "\n", os);
		os.close();

		FSDataInputStream is = fs.open(new Path(CommonDemo.root() + DEMO + SHAKESPEARE_TXT));
		List<String> read = IOUtils.readLines(is);
		is.close();

		if (text.size() != read.size()) {
			CommonDemo.fail(DEMO, "Error: expected " + text.size() + " lines but read " + read.size());
		}
		for (int i = 0; i < read.size(); i ++) {
			if (!read.get(i).equals(text.get(i))) {
				CommonDemo.fail(DEMO, "Error: expected '" + text.get(i) + "' but read '" + read.get(i) + "' on line " + i);
			}
		}
		CommonDemo.succeed(DEMO);
	}

}

