package com.cisco.shippedanalytics.demos.cassandra

import com.datastax.driver.core.Session
import com.datastax.driver.core.Cluster
import scala.io.Source
import scala.collection.JavaConversions._

/**
 * Basic demo showing that Cassandra is installed and running.
 *
 */
object Run extends App {

case class Config(node: String = "127.0.0.1",
port: Int = 9042,
mode: String = "r",
interval: Int = 10,
debug: Boolean = false)

val parser = new scopt.OptionParser[Config]("Shipped Analytics - Cassandra Demo") {
	opt[String]('n', "node") required() action { (x, c) => c.copy(node = x)} text "Cassandra node IP or FQDN, 127.0.0.1 by default"
	opt[String]('m',"mode") required() action { (x, c) => c.copy(mode = x)} text "Application mode, 'r' for Reader and 'w' for Writer"
	opt[Int]('p',"port") required() action { (x, c) => c.copy(port = x)} text "Cassandra node native transport port, usually 9042"
	opt[Int]('i',"interval") required() action { (x, c) => c.copy(port = x)} text "interval in seconds for fetching load average data (for writer mode)"
	opt[Boolean]('d',"debug") action { (x, c) => c.copy(debug = x)} text "Debug mode, 'false' by default"
}

def quitWithError(): Unit ={
		parser.showUsage
		System.exit(1)
}

parser.parse(args, Config()) map { config =>
if (config.debug)
	printf("Cassandra Sample Scala App\nNode: %s, Port: %d, Mode: %s, debug enabled.\n",
			config.node,
			config.port,
			config.mode match {
			case "r" => "reader"
			case "w" => "writer"
	})

	if (!(0 < config.port && config.port < 65535)) {
		printf("Incorrect port %d specified.\n", config.port)
		quitWithError()
	}

config.mode match {
case "r" => val cas = read(config.node, config.port, config.debug)
case "w" => val cas = write(config.node, config.port, config.interval, config.debug)
case default => {
	printf("Incorrect mode \"%s\"\n", config.mode)
	quitWithError()
}
}
}

def read(node: String, port: Int, debug: Boolean) {

	var query = "SELECT dateOf(ts), host, m1, m5, m10 FROM load_averages;";
	printf("Reader module started with node '%s' and port %d\n", node, port)

	val cluster = Cluster.builder().withPort(port).addContactPoint(node).build()
	val session = cluster.connect("demo")

	if (debug) {
		val metadata = cluster.getMetadata;
		printf("Connected to cluster: %s\n", metadata.getClusterName)

		for (host <- metadata.getAllHosts) {
			printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter, host.getAddress, host.getRack)
		}
		println("\n")
	}

	val rows = session.execute(query);

	println("Load average format:\n| hostname |\tdate |\t1 minute |\t5 minutes |\t10 minutes |\nData:");

	for (row <- rows) {
		printf("| %s |\t%s |\t%.2f |\t%.2f |\t%.2f |\n",
				row.getString("host"),
				row.getDate("dateOf(ts)").toString,
				row.getFloat("m1"),
				row.getFloat("m5"),
				row.getFloat("m10")
				)
	}

}

def formQuery(m1: Float, m5: Float, m10: Float, hostname : String): String = {
		"INSERT INTO load_averages (m1, m5, m10, host, ts) " +
				s"VALUES ($m1, $m5, $m10, '$hostname', now() )"
}

def write(node: String, port: Int, interval: Int,  debug: Boolean) {

	val avgfile = "/proc/loadavg";
	val hostname = java.net.InetAddress.getLocalHost.getHostName;

	printf("Writer module started with node '%s' and port %d\n", node, port)

	val cluster = Cluster.builder().withPort(port).addContactPoint(node).build()
	val session = cluster.connect("demo")

	if (debug) {
		val metadata = cluster.getMetadata;
		printf("Connected to cluster: %s\n", metadata.getClusterName)

		for (host <- metadata.getAllHosts) {
			printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter, host.getAddress, host.getRack)
		}
		println("\n")
	}

	while (true) {
		val avg_data = Source.fromFile(avgfile).getLines.next.split(' ');
		if (debug) {
			println(formQuery(avg_data(0).toFloat, avg_data(1).toFloat, avg_data(2).toFloat, hostname))
		}
		session.execute(formQuery(avg_data(0).toFloat, avg_data(1).toFloat, avg_data(2).toFloat, hostname))
		Thread.sleep(interval * 1000)
	}

}

}
