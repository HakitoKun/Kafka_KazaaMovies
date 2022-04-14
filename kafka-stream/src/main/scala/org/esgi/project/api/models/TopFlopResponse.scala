package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class TopFlopResponse(
                          title: String,
                          views: Long
                          )


object TopFlopResponse {
  implicit val format: OFormat[TopFlopResponse] = Json.format[TopFlopResponse]
}

