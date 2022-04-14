package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class Stat(
                 start_only: String,
                 half : String,
                 full: String
               )

object StatResponse {
  implicit val format: OFormat[Stat] = Json.format[Stat]
}