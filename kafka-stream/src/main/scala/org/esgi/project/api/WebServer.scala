package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, MovieViewResponse}
import org.esgi.project.streaming.StreamProcessing

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * -------------------
 * Part.3 of exercise: Interactive Queries
 * -------------------
 */
object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("movies" /  Segment) { period: String =>
        get {
          val isNumber : Boolean = period.forall(Character.isDigit)
          isNumber match {
            case true =>
              // Start of the movie
              val kvStoppedAtStartOfTheMovieSinceStart: ReadOnlyKeyValueStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtStartOfTheMovieSinceStartStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvStoppedAtStartOfTheMovieLastMinute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtMiddleOfTheMovieLastMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvStoppedAtStartOfTheMovieLastFiveMinutes: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtMiddleOfTheMovieLastFiveMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // Half of the movie
              val kvStoppedAtMiddleOfTheMovieSinceStart: ReadOnlyKeyValueStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtStartOfTheMovieSinceStartStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvStoppedAtMiddleOfTheMovieLastMinute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtMiddleOfTheMovieLastMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvStoppedAtMiddleOfTheMovieLastFiveMinute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.stoppedAtMiddleOfTheMovieLastFiveMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // End of the movie
              val kvFinishedTheMovieSinceStart: ReadOnlyKeyValueStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.finishedTheMovieSinceStartStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvFinishedTheMovieLastMinute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.finishedTheMovieLastMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              val kvFinishedTheMovieLastFiveMinute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.finishedTheMovieLastFiveMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))

              // Récupération des valeurs :

              val kaStoppedAtStartOfTheMovieSinceStart : List[KeyValue[String, Long]] = kvStoppedAtStartOfTheMovieSinceStart.all.asScala.toList
              val kaStoppedAtMiddleOfTheMovieSinceStart : List[KeyValue[String, Long]] = kvStoppedAtMiddleOfTheMovieSinceStart.all.asScala.toList
              val kaFinishedTheMovieSinceStart : List[KeyValue[String, Long]] = kvFinishedTheMovieSinceStart.all.asScala.toList

              // Last Minute

              val timeFrom : Instant = Instant.now.minusSeconds(60) // Beginning of the time = Oldest value available.
              val timeTo : Instant = Instant.now

              val kaStoppedAtStartOfTheMovieLastMinute : List[KeyValue[Windowed[String], Long]] = kvStoppedAtStartOfTheMovieLastMinute.fetchAll(timeFrom, timeTo).asScala.toList
              val kaStoppedAtMiddleOfTheMovieLastMinute : List[KeyValue[Windowed[String], Long]] = kvStoppedAtMiddleOfTheMovieLastMinute.fetchAll(timeFrom, timeTo).asScala.toList
              val kaFinishedTheMovieLastMinute : List[KeyValue[Windowed[String], Long]] = kvFinishedTheMovieLastMinute.fetchAll(timeFrom, timeTo).asScala.toList

              // Last Fives Minutes

              val timeFrom_2 : Instant = Instant.now.minusSeconds(60 * 5)
              //val kvStoppedAtStartOfTheMovieLastMinute =

              val kaStoppedAtStartOfTheMovieLastFive : List[KeyValue[Windowed[String], Long]] = kvStoppedAtStartOfTheMovieLastFiveMinutes.fetchAll(timeFrom_2, timeTo).asScala.toList
              val kaStoppedAtMiddleOfTheMovieLastFiveMinute : List[KeyValue[Windowed[String], Long]] = kvStoppedAtMiddleOfTheMovieLastFiveMinute.fetchAll(timeFrom_2, timeTo).asScala.toList
              val kaFinishedTheMovieLastFiveMinute : List[KeyValue[Windowed[String], Long]] = kvFinishedTheMovieLastFiveMinute.fetchAll(timeFrom_2, timeTo).asScala.toList

              complete(
                List(MovieViewResponse(0, "", 0, null, null, null))
              )
            case false =>
              complete(
                // TODO: output a list of MeanLatencyForURLResponse objects
                List(MeanLatencyForURLResponse("", 0))
              )
          }
        }
      },
      path("stats" / "ten" / "best" / Segment ) { affinity : String =>
        get {
          affinity match {
            case "score" => ???
            case "views" => ???
            case _ =>
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },
      path("stats" / "ten" / "worst" / Segment ) { affinity : String =>
        get {
          affinity match {
            case "score" => ???
            case "views" => ???
            case _ =>
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      }
    )
  }
}
