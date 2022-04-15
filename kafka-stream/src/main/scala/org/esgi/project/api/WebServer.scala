package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{MovieViewResponse, StatResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.MovieStat

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

              val kvLastMinute : ReadOnlyWindowStore[Long, MovieStat] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.movieLastMinuteStoreName, QueryableStoreTypes.windowStore[Long, MovieStat]()))

              val kvLastFiveMinute : ReadOnlyWindowStore[Long, MovieStat] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.movieLastFiveMinutesStoreName, QueryableStoreTypes.windowStore[Long, MovieStat]()))


              val valueFromStart = kvLastMinute.all().asScala
              .filter(keyValue => keyValue.key.key() == period.toLong)
                .toList
                .foldLeft(StatResponse.empty)(
                  (acc, keyValue) => acc.accumulator(
                    keyValue.value.start_only,
                    keyValue.value.half,
                    keyValue.value.full
                  )
                )

              val timeFrom60 : Instant = Instant.now.minusSeconds(60) // Beginning of the time = Oldest value available.
              val timeFrom60times5 : Instant = Instant.now.minusSeconds(60 * 5)
              val timeTo : Instant = Instant.now

              val valueLastMinutes = kvLastMinute
                .fetch(period.toLong, timeFrom60, timeTo)
                .asScala
                .foldLeft(StatResponse.empty)(
                  (acc, keyValue) => acc.accumulator(
                    keyValue.value.start_only,
                    keyValue.value.half,
                    keyValue.value.full)
                )

              val valueLastFiveMinutes = kvLastFiveMinute
                .fetch(period.toLong, timeFrom60times5, timeTo)
                .asScala
                .foldLeft(StatResponse.empty)(
                  (acc, keyValue) => acc.accumulator(
                    keyValue.value.start_only,
                    keyValue.value.half,
                    keyValue.value.full)
                )

              val movieTitle = kvLastMinute.all().asScala
              .filter(keyValue => keyValue.key.key() == period.toLong)
              .toList
                .map(x => x.value.title).distinct



          complete(
                MovieViewResponse(
                  period.toLong,
                  movieTitle.head,
                  valueFromStart.start_only+valueFromStart.half+valueFromStart.full,
                  StatResponse(
                    valueFromStart.start_only,
                    valueFromStart.half,
                    valueFromStart.full),
                  StatResponse(
                    valueLastMinutes.start_only,
                    valueLastMinutes.half,
                    valueLastMinutes.full),
                  StatResponse(
                    valueLastFiveMinutes.start_only,
                    valueLastFiveMinutes.half,
                    valueLastFiveMinutes.full)
                )
              )
            case false =>
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
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
            case "score" =>
              /*val kvTop10BestScore: ReadOnlyKeyValueStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(
                  StreamProcessing.top10BestScoreStoreName, QueryableStoreTypes.()))
*/          complete(
              HttpResponse(StatusCodes.NotFound, entity = "Not found")
            )
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
