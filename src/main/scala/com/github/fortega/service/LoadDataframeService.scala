package com.github.fortega.service

import org.apache.spark.sql.DataFrame
import com.github.fortega.model.OutputDefinition

object LoadDataframeService {
  def apply(
      data: DataFrame,
      output: Option[OutputDefinition]
  ): Option[Throwable] = output match {
    case None => {
      data.show(false)
      None
    }
    case Some(output) =>
      try {
        data.write
          .mode(output.mode)
          .format(output.format)
          .options(output.options)
          .save
        None
      } catch { case e: Throwable => Some(e) }
  }
}
