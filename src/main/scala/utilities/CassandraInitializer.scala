package utilities

import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.cql.CassandraConnector

object CassandraInitializer {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("Load All Information to Cassandra")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "10.1.51.42")
//      .set("spark.cassandra.connection.host", args(0).toString)
    val sc = new SparkContext(conf)

    val cassandraConnector = CassandraConnector.apply(conf)

    cassandraConnector.withSessionDo(session => {
      session.execute(s"""DROP KEYSPACE IF EXISTS sparkks;""")
      session.execute("CREATE KEYSPACE IF NOT EXISTS sparkks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ")

      session.execute(s"""CREATE TABLE IF NOT EXISTS sparkks.tweetdump (
            userid text,
						latitude bigint,
						longitude bigint,
						tweet text,
						tweetdate timestamp,
						PRIMARY KEY (userid, tweetdate)
					)
					;""")
      System.out.println("==================================================================================")

//      session.execute(s"""CREATE TABLE IF NOT EXISTS sparkks.positivewords (
//            word text,
//						PRIMARY KEY (word)
//					)
//					;""")
//      System.out.println("==================================================================================")
//
//
//
//      session.execute(s"""CREATE TABLE IF NOT EXISTS sparkks.negativewords (
//            word text,
//						PRIMARY KEY (word)
//					)
//					;""")
//
//      System.out.println("==================================================================================")



      session.execute(s"""TRUNCATE TABLE sparkks.tweetdump;""")
//      session.execute(s"""TRUNCATE TABLE sparkks.positivewords;""")
//      session.execute(s"""TRUNCATE TABLE sparkks.negativewords;""")
//
//      session.execute(s"""COPY sparkks.positivewords from 'C:\\Users\\prasad\\Downloads\\TwitterLocationAnalytics-master\\src\\main\\resources\\wordbanks\\positive-words.txt';""")
//      session.execute(s"""COPY sparkks.negativewords from 'C:\\Users\\prasad\\Downloads\\TwitterLocationAnalytics-master\\src\\main\\resources\\wordbanks\\negative-words.txt';""")
//
//
    })
    System.out.println("==================================================================================")
    System.out.println("Created Keyspace 'sparkks' with tables 'tweetdump', 'positivewords', 'negativewords' ")
//    System.out.println("Truncated tables 'bidoffertrade_from_consumer' 'bidoffertrade_from_consumer_sec' 'bidoffertrade_from_consumer_min' ")
    System.out.println("==================================================================================")

    sc.stop()
  }
}