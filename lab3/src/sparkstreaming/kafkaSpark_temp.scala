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

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    // val cluster = Cluster.<FILL IN>
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    // session.execute(.<FILL IN>)
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float)")


    // make a connection to Kafka and read (key, value) pairs from it
    // connect Spark to Kafka in the receiver-less direct approach where Spark periodically queries Kafka for the latest offsets in each topic + partition
    // val kafkaConf = <FILL IN>
	val sparkConf = new SparkConf().setAppName("KafkaSparkAverageValue").setMaster("local[2]")
	val ssc = new StreamingContext(sparkConf, Seconds(1))  // the 1 in Seconds seems to be a magical number   
    val kafkaConf = Map(
    	"metadata.broker.list" -> "localhost:9092",
    	"zookeeper.connect" -> "localhost:2181",
    	"group.id" -> "kafka-spark-streaming",
    	"zookeeper.connection.timeout.ms" -> "1000"
    )
    val topicsSet = "avg".toSet
    
    val kafkaStream = KafkaUtils.createDirectStream[String, Integer, StringDecoder, IntegerDecoder](
    	ssc, kafkaConf, topicsSet)

	// https://blog.csdn.net/zhoudetiankong/article/details/77504026
	// It seems that we can directly apply map reduce on the code, however, I am not sure how to operate on the event yet.


	val pairs = kafkaStream.map(x => (x._1, x._2))



    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    // <FILL IN>
	    val result
	    return result
    }

    // initialize an empty state RDD 
    val initialRDD = ssc.sparkContext.parallelize(List())


    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    // TODO: store the result in Cassandra
    // But where should I store it though?? Maybe within the mapping Func?         
    stateDstream.print()


    ssc.start()
    ssc.awaitTermination()


    // 	https://github.com/eBay/Spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/StatefulNetworkWordCount.scala

    // https://apache.googlesource.com/spark/+/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaStatefulNetworkWordCount.java
  }
}
