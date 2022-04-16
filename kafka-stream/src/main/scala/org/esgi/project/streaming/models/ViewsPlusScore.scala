package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewsPlusScore(
                  _id: Long,
                  title: String,
                  view_category: String,
                  score: Double
                )
object ViewsPlusScore {
  implicit val format: OFormat[ViewsPlusScore] = Json.format[ViewsPlusScore]
}
