package com.github.fortega

import com.github.fortega.adapter.JsonConfigAdapter
import scala.util.Failure
import scala.util.Success
import org.slf4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import com.github.fortega.model.Config
import com.github.fortega.model.ViewDefinition
import com.github.fortega.model.OutputDefinition
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fortega.service.ConfigValidationService
import com.github.fortega.adapter.SparkFromEnvAdapter
import com.github.fortega.service.ExtractToViewService
import scala.collection.immutable

object App {
  lazy val logger = LoggerFactory.getLogger("App")

  def saveDataFrame(
      output: OutputDefinition,
      data: DataFrame
  ): Unit = data.write
    .mode(output.mode)
    .format(output.format)
    .options(output.options)
    .save

  def handleError(
      name: String,
      error: Throwable,
      exit: Boolean = true
  ): Unit = {
    logger.error(s"ERROR @ $name: ${error.getMessage}")
    if (exit) sys.exit(1)
  }

  def runEtl(config: Config, spark: SparkSession): Unit = {
    ExtractToViewService(spark)(config.inputs) match {
      case error :: tail => handleError("extract", error)
      case Nil =>
        val result = spark.sql(config.sql)

        config.output match {
          case None         => result.show(false)
          case Some(output) => saveDataFrame(output, result)
        }
    }
  }

  def main(cmdArgs: Array[String]): Unit = {
    JsonConfigAdapter(cmdArgs) match {
      case Failure(error) => handleError("config", error)
      case Success(config) =>
        ConfigValidationService(config) match {
          case errors: List[String] if (errors.nonEmpty) =>
            errors.foreach(logger.error)
          case _ =>
            SparkFromEnvAdapter() match {
              case Failure(error) => handleError("spark", error)
              case Success(spark) => runEtl(config, spark)
            }
        }
    }
  }
}
