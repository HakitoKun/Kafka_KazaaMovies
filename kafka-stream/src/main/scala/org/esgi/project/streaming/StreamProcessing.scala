package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{Likes, MovieStat, Views, ViewsPlusScore}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._


  val applicationName = s"Kazaa-Movies"


  val viewTopicName: String = "views"
  val likesTopicName: String = "likes"

  // WindowsPeriode

  val movieSinceStartStoreName: String = "movieSinceStart"
  val movieLastMinuteStoreName: String = "movieLastMinute"
  val movieLastFiveMinutesStoreName: String = "movieLastFiveMinutes"

  // Top 10 / Flop 10

  val meanScoreMoveStoreName : String = "meanScoreMovie"
  val totalViewByMovieStoreName : String = "totalViewByMovie"

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
  // TODO: A CORRIGER

  val groupedByTitle : KGroupedStream[Long, Views] = {
    views
      .groupBy((_, value) => value._id)
  }

  val lastMinute : KTable[Windowed[Long], MovieStat] =
    groupedByTitle
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60))
      .advanceBy(Duration.ofSeconds(60)))
    .aggregate(MovieStat.empty)(
      (_, v, agg) => agg.increment(v.view_category).defineTitle(v.title)
    )(Materialized.as(movieLastMinuteStoreName))

  val lastFiveMinute : KTable[Windowed[Long], MovieStat] =
    groupedByTitle
      .windowedBy(
        TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60 * 5))
        .advanceBy(Duration.ofSeconds(60 * 5)))
      .aggregate(MovieStat.empty)(
        (_, v, agg) => agg.increment(v.view_category).defineTitle(v.title)
      )(Materialized.as(movieLastFiveMinutesStoreName))

  val totalView : KTable[String, Long] =
    views
      .groupBy((_, v) => v.title)
      .count()(Materialized.as(totalViewByMovieStoreName))


  /// Arret en début de film



  // Top 10 - Flop 10

  val joinWithScore: KStream[String, ViewsPlusScore] = views.join(likes)({(view, like) =>
    ViewsPlusScore(view._id, view.title, view.view_category, like.score)
  }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))

  // TOP 10
  // val top10BestFeedback =
  // val flop10WorstFeedback =



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
