package com.github.fortega.adapter

import com.github.fortega.port.SparkPort
import org.apache.spark.sql.SparkSession
import scala.util.Try

object SparkFromEnvAdapter {
  private val LOCAL_SPARK = "local[*]"
  def apply(
      isLocal: Boolean = !sys.env.contains("SPARK_ENV")
  ): SparkPort = Try {
    val builder = SparkSession.builder
    if (isLocal) builder.master(LOCAL_SPARK).getOrCreate
    else builder.getOrCreate
  }
}
