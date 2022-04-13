package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{Likes, Views}

import java.io.InputStream
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._


  val applicationName = s"Kazaa-Movies"


  val viewTopicName: String = "view"
  val likesTopicName: String = "likes"

  // Arret en début de filme

  val stoppedAtStartOfTheMovieSinceStartStoreName: String = "stoppedAtStartOfTheMovieSinceStart"
  val stoppedAtStartOfTheMovieLastMinuteStoreName: String = "stoppedAtStartOfTheMovieLastMinute"
  val stoppedAtStartOfTheMovieLastFiveMinutesStoreName: String = "stoppedAtStartOfTheMovieLastFiveMinutes"

  // Arret en millieu de film

  val stoppedAtMiddleOfTheMovieSinceStartStoreName: String = "stoppedAtMiddleOfTheMovieSinceStart"
  val stoppedAtMiddleOfTheMovieLastMinuteStoreName: String = "stoppedAtMiddleOfTheMovieLastMinute"
  val stoppedAtMiddleOfTheMovieLastFiveMinuteStoreName: String = "stoppedAtMiddleOfTheMovieLastFiveMinute"

  // Film terminé

  val finishedTheMovieSinceStartStoreName: String = "finishedTheMovieSinceStart"
  val finishedTheMovieLastMinuteStoreName: String = "finishedTheMovieLastMinute"
  val finishedTheMovieLastFiveMinuteStoreName: String = "finishedTheMovieLastFiveMinute"

  // Top 10 / Flop 10

  val top10BestFeedbackStoreName : String = "top10BestFeedback"
  val flop10WorstFeedbackStoreName : String = "flop10WorstFeedback"


  val props = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // declared topic sources to be used
  val views: KStream[String, Views] = builder.stream[String, Views](viewTopicName)
  val likes: KStream[String, Likes] = builder.stream[String, Likes](likesTopicName)

  /**
   * -------------------
   * Part.1 of exercise
   * -------------------
   */
  // TODO: repartition visits per URL
  val stoppedAtStartOfTheMovieSinceStart: KGroupedStream[String, Views] = ???

  // TODO: implement a computation of the visits count per URL for the last 30 seconds,
  // TODO: the last minute and the last 5 minutes
  val visitsOfLast30Seconds: KTable[Windowed[String], Long] = ???

  val visitsOfLast1Minute: KTable[Windowed[String], Long] = ???

  val visitsOfLast5Minute: KTable[Windowed[String], Long] = ???

  /**
   * -------------------
   * Part.2 of exercise
   * -------------------
   */
  // TODO: repartition visits topic per category instead (based on the 2nd part of the URLs)
  val visitsGroupedByCategory: KGroupedStream[String, Visit] = ???

  // TODO: implement a computation of the visits count per category for the last 30 seconds,
  // TODO: the last minute and the last 5 minutes
  val visitsOfLast30SecondsByCategory: KTable[Windowed[String], Long] = ???

  val visitsOfLast1MinuteByCategory: KTable[Windowed[String], Long] = ???

  val visitsOfLast5MinuteByCategory: KTable[Windowed[String], Long] = ???

  // TODO: implement a join between the visits topic and the metrics topic,
  // TODO: knowing the key for correlated events is currently the same UUID (and the same id field).
  // TODO: the join should be done knowing the correlated events are emitted within a 5 seconds latency.
  // TODO: the outputted message should be a VisitWithLatency object.
  val visitsWithMetrics: KStream[String, VisitWithLatency] = ???

  // TODO: based on the previous join, compute the mean latency per URL
  val meanLatencyPerUrl: KTable[String, MeanLatencyForURL] = ???

  // -------------------------------------------------------------
  // TODO: now that you're here, materialize all of those KTables
  // TODO: to stores to be able to query them in Webserver.scala
  // -------------------------------------------------------------

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run {
        streams.close
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}
