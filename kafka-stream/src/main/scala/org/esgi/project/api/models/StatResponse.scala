package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class StatResponse(
                 start_only: Long,
                 half : Long,
                 full: Long
               ){
  def accumulator(start_only: Long, half: Long, full: Long): StatResponse = {
    this.copy(
      start_only = this.start_only + start_only,
      half = this.half + half,
      full = this.full + full
    )
  }
}

object StatResponse {
  implicit val format: OFormat[StatResponse] = Json.format[StatResponse]

  def empty : StatResponse = StatResponse(start_only = 0, half = 0, full = 0)

}