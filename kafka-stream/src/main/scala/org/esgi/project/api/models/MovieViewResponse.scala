package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}
case class MovieViewResponse(
                            _id: Long,
                            title: String,
                            view_count: Long,
                            past : Stat,
                            last_minute: Stat,
                            last_five_minutes: Stat
                            )


object MovieViewResponse {
  implicit val format: OFormat[MovieViewResponse] = Json.format[MovieViewResponse]
}