package com.github.fortega

import com.github.fortega.model.{Config, ViewDefinition, OutputDefinition}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Try

package object port {
  type ConfigPort = Try[Config]
  type SparkPort = Try[SparkSession]
}
