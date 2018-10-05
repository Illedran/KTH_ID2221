package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


// define a class to store the visiting state of each key-value pair
case class VisitState(count: Int = 0, sum: Double = 0.0) {
  // stores the average visit count
  val avg = sum / scala.math.max(count, 1)

  // define the add operator of the class
  def +(count: Int, sum: Double) = VisitState (
    this.count + count,
    this.sum + sum
  )

  // toString method
  override def toString = {
    s"Visit($count, $sum, $avg)"
  }
}

object KafkaSpark {
  def main(args: Array[String]) {
    
    // connect to Cassandra and make a keyspace and table as explained in the document

    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float)")


    // make a connection to Kafka and read (key, value) pairs from it
    // connect Spark to Kafka in the receiver-less direct approach where Spark periodically queries Kafka for the latest offsets in each topic + partition

	val sparkConf = new SparkConf().setAppName("KafkaSparkAverageValue").setMaster("local[2]")
	val ssc = new StreamingContext(sparkConf, Seconds(1))
    val kafkaConf = Map(
    	"metadata.broker.list" -> "localhost:9092",
    	"zookeeper.connect" -> "localhost:2181",
    	"group.id" -> "kafka-spark-streaming",
    	"zookeeper.connection.timeout.ms" -> "1000"
    )

    val topicsSet = Set("avg")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    	ssc, kafkaConf, topicsSet)

    // extract useful information as the origianl key-value pair are of the format (null, "a, 10")
	val lines = kafkaStream.map(x => x._2)
	val arr = lines.map(x => x.split(","))
	val pairs = arr.map(record => (record(0), record(1).toDouble))
 

 	// define the maping function
 	// State[Object] will retain its value across different mini batch
    def mappingFunc(key: String, value: Option[Double], state: State[VisitState]): (String, Double) = {
    	val delta = value.getOrElse(0.0)
    	val prev = state.getOption.getOrElse(VisitState(0, 0.0))
    	val current = prev + (1, delta)
    	state.update(current)
    	(key, current.avg)
    	// the following return code is for debugging purpose and should chang the return value of the function from (String, Double) => (String)
    	// current.toString
    }


    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // print to screen
    stateDstream.print()

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.checkpoint(".")
    ssc.start()
    ssc.awaitTermination()
  }
}
