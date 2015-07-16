package com.cisco.shippedanalytics.demos.cassandra

import com.datastax.driver.core.Session
import com.datastax.driver.core.Cluster
import scala.io.Source
import scala.collection.JavaConversions._
import java.util.logging.Logger
import java.util.Formatter.DateTime
import java.util.Formatter.DateTime
import java.util.Date
import java.text.DateFormat

/**
 * Basic demo showing that Cassandra is installed and running.
 *
 */
object Run extends App {

case class Config (
		node: String = "127.0.0.1",
		port: Int = 9042,
		interval: Int = 1)

		override def main(args: Array[String]) {

		val logger = Logger.getLogger(Run.getClass.getName());

		val parser = new scopt.OptionParser[Config]("Shipped Analytics - Cassandra Demo") {
			opt[String]('n', "node") action { (x, c) => c.copy(node = x)} text "Cassandra node IP or FQDN, 127.0.0.1 by default"
			opt[Int]('p',"port") action { (x, c) => c.copy(port = x)} text "Cassandra node native transport port, 9042 by default"
			opt[Int]('i',"interval") action { (x, c) => c.copy(port = x)} text "interval in seconds for fetching load average data (for writer mode), 1 by default"
		};

		parser.parse(args, Config()) match {
		case Some(config) => {

			val host = java.net.InetAddress.getLocalHost.getHostName;
			val signature = DateFormat.getDateTimeInstance().format(new Date());
			val iterations = 10;

			{
				// connecting and setup
				logger.info("Starting with Cassandra node: " + config.node + ":" + config.port);
        val cluster = Cluster.builder().withPort(config.port).addContactPoint(config.node).build();

				for (host <- cluster.getMetadata.getAllHosts) {
					logger.info("Datacenter: " + host.getDatacenter + ", Host: " + host.getAddress + ", Rack: " + host.getRack);
				}

				cluster.connect().execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
				val session = cluster.connect("demo");
				session.execute("CREATE TABLE IF NOT EXISTS demo.load_averages(nr int, lastMinuteAverage float, host text, signature text, PRIMARY KEY (nr, host, signature))");

				// writing data
				logger.info("Cassandra table created and start writing to " + cluster.getMetadata.getClusterName);
				val avgfile = "/proc/loadavg";

				for (i <- 1 to iterations) {
					val avg_data = Source.fromFile(avgfile).getLines.next.split(' ')(0);
					session.execute("INSERT INTO demo.load_averages (nr, lastMinuteAverage, host, signature) VALUES (" + i + ", " + avg_data + ", '" + host + "', '" + signature + "')");
					Thread.sleep(config.interval * 1000)
				}
			}

			{
				// reading data
				logger.info("Finished writing, start reading");
				val cluster = Cluster.builder().withPort(config.port).addContactPoint(config.node).build();
				val session = cluster.connect("demo");
				val rows = session.execute("SELECT lastMinuteAverage, host, signature FROM demo.load_averages WHERE host = '" + host + "' AND signature = '" + signature + "' ALLOW FILTERING");

				var errorMessage = "";

        var count = 0;
        var data = "\nRetrieved results: lastMinuteAverage | host | signature\n";

				for (row <- rows) {
					val readLoad = row.getFloat("lastMinuteAverage");
					val readHost = row.getString("host");
					val readSignature = row.getString("signature");
					data = data + s"$readLoad | $readHost | $readSignature\n";
					if (!readHost.equals(host)) {
						errorMessage = errorMessage + s"Read host '$readHost' while '$host' was expected\n";
					}
					if (!readSignature.equals(signature)) {
						errorMessage = errorMessage + s"Read signature '$readSignature' while '$signature' was expected\n";
					}
          count = count + 1;
				}

        logger.info(data);
        if (count != iterations) {
          errorMessage = "Read " + count + " records but " + iterations + " were expected\n";
        }

				if (errorMessage.isEmpty()) {
					logger.info("SUCCESS");
					System.exit(0);
				} else {
					logger.info("FAILED");
					logger.severe(errorMessage);
					System.exit(1);
				}
			}
		}
		case None =>
      logger.info(parser.usage)
		}
	}
}
