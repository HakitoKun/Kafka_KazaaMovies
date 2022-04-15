package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreFilm(
                             sum: Double,
                             count: Double,
                             meanScore: Double,
                             title: String
                           ) {

  def increment(score: Double) = this.copy(
    sum = this.sum + score, count = this.count + 1
  )

  def computeMean = this.copy(
    meanScore = this.sum / this.count
  )

  def attributeTitle(title: String) = this.copy(
    title = title
  )
}

object MeanScoreFilm {
  implicit val format: OFormat[MeanScoreFilm] = Json.format[MeanScoreFilm]

  def empty: MeanScoreFilm = MeanScoreFilm(0, 0, 0, "")
}
