package com.cisco.shippedanalytics.demos.sparkcassandra;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;


/**
 * Access log record.
 *
 * @author Borys Omelayenko
 *
 */
@Table(keyspace = "demos", name = "logRecord")
public class LogRecord {

	@PartitionKey(0)
	private String ip;

	private String user;

	@PartitionKey(1)
	private Integer time;

	private String url;

	public LogRecord(String ip, String user, Integer time, String url) {
		this.ip = ip;
		this.user = user;
		this.time = time;
		this.url = url;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Integer getTime() {
		return time;
	}

	public void setTime(Integer time) {
		this.time = time;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}


}
