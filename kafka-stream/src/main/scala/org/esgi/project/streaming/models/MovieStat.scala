package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MovieStat(
                    title: String,
                    half: Long,
                    full: Long,
                    start_only: Long,
                    total: Long
                    ) {
  def increment(view_category:String): MovieStat = {
    view_category match {
      case "start_only" => this.copy(start_only = start_only + 1, total = total + 1)
      case "half" => this.copy(half = half + 1, total = total + 1)
      case "full" => this.copy(full = full + 1, total = total + 1)
      case _ => this.copy()
    }
  }

  def defineTitle(title : String): MovieStat = {
    this.copy(title = title)
  }
}

/**
 * Define an Empty Movie Stat
 */
object MovieStat{
  def empty :MovieStat = MovieStat("0", 0, 0, 0, 0)

  implicit val format: OFormat[MovieStat] = Json.format[MovieStat]
}