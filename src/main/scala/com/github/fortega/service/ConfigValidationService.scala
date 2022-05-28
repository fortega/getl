package com.github.fortega.service

import com.github.fortega.model.Config
import scala.collection.immutable

object ConfigValidationService {
  def apply(config: Config): List[String] =
    List(
      "inputs not defined" -> (config.inputs == null || config.inputs.isEmpty),
      "sql not defined" -> (config.sql == null || config.sql.isEmpty)
    ).flatMap { case (k, v) =>
      if (v) Some(k) else None
    }
}
