package com.cisco.shippedanalytics.demos.sparkcassandra;

import java.util.Date;

/**
 * Access log record.
 *
 * @author Borys Omelayenko
 *
 */
public class LogRecord {

	private String ip;

	private String user;

	private Date time;

	private String url;

	public LogRecord(String ip, String user, Date time, String url) {
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

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}


}
