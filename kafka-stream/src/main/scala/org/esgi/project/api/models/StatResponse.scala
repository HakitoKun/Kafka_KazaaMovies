package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class StatResponse(
                 start_only: Long,
                 half : Long,
                 full: Long
               )

object StatResponse {
  implicit val format: OFormat[StatResponse] = Json.format[StatResponse]
}