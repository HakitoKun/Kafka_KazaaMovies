package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{Likes, Views, ViewsPlusScore}
import java.io.InputStream
import java.time.Duration
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

  val top10FeedbackStoreName : String = "top10Feedback"
  val flop10FeedbackStoreName : String = "flop10Feedback"


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

  val groupedByTitle : KStream[String, Views] =
  views.map((_, views) => (views.title, views))

  /// Arret en début de film

  val startOnly: KGroupedStream[String, Views] = groupedByTitle.filter((_, views) => views.view_category.equals("start_only")).groupBy((_, v)=> v.title)

  val stoppedAtStartOfTheMovieSinceStart : KTable[String, Long] =
    startOnly.count()(Materialized.as(stoppedAtStartOfTheMovieSinceStartStoreName))

  val stoppedAtStartOfTheMovieLastMinute : KTable[Windowed[String], Long] = startOnly.windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
      ).count()(Materialized.as(stoppedAtMiddleOfTheMovieLastMinuteStoreName))

  val stoppedAtStartOfTheMovieLastFiveMinutes: KTable[Windowed[String], Long] = startOnly.windowedBy(
    TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(5))
  ).count()(Materialized.as(stoppedAtStartOfTheMovieLastFiveMinutesStoreName))

  //Arret en millieu de film

  val half: KGroupedStream[String, Views] = groupedByTitle.filter((_, views) => views.view_category.equals("half")).groupBy((_, v)=> v.title)

  val stoppedAtMiddleOfTheMovieSinceStart : KTable[String, Long] =
    half.count()(Materialized.as(stoppedAtMiddleOfTheMovieSinceStartStoreName))

  val stoppedAtMiddleOfTheMovieLastMinute : KTable[Windowed[String], Long] = half.windowedBy(
    TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
  ).count()(Materialized.as(stoppedAtMiddleOfTheMovieLastMinuteStoreName))

  val stoppedAtMiddleOfTheMovieLastFiveMinutes: KTable[Windowed[String], Long] = half.windowedBy(
    TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(5))
  ).count()(Materialized.as(stoppedAtStartOfTheMovieLastFiveMinutesStoreName))

  //  Film terminé
  val finish: KGroupedStream[String, Views] = groupedByTitle.filter((_, views) => views.view_category.equals("full")).groupBy((_, v)=> v.title)

  val finishedTheMovieSinceStart : KTable[String, Long] = finish.count()(Materialized.as(finishedTheMovieSinceStartStoreName))

  val finishedTheMovieLastMinute: KTable[Windowed[String], Long] = finish.windowedBy(
    TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(stoppedAtStartOfTheMovieLastMinuteStoreName))

  val finishedTheMovieLastFiveMinute : KTable[Windowed[String], Long] = finish.windowedBy(
    TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(finishedTheMovieLastFiveMinuteStoreName))

  // Top 10 - Flop 10

  val joinWithScore: KStream[String, ViewPlusScore] = views
    .join(likes)({ (view, like) =>
      ViewPlusScore(view._id, view.title, view.view_category, like.score)
    }, JoinWindows.of(Duration.ofSeconds(5)))


  // TOP 10
  val top10BestFeedback =
  val flop10WorstFeedback =



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
