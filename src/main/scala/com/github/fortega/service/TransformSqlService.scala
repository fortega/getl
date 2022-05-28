package com.github.fortega.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Try

object TransformSqlService {
  def apply(
      spark: SparkSession
  ): String => Try[DataFrame] =
    sqlText => Try { spark.sql(sqlText) }
}
