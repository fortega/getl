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
import com.github.fortega.service.TransformSqlService
import com.github.fortega.service.LoadDataframeService

object App {
  lazy val logger = LoggerFactory.getLogger("App")

  private def handleError(
      name: String,
      error: Throwable
  ): Unit = handleError(name, List(error))

  private def handleError(
      name: String,
      errors: List[Throwable]
  ): Unit = {
    errors.foreach { error => logger.error(s"ERROR @ $name", error) }
    sys.exit(1)
  }

  private def handleSuccess: Unit = {
    logger.info("SUCCESS")
    sys.exit(0)
  }

  def runEtl(config: Config, spark: SparkSession): Unit = {
    ExtractToViewService(spark)(config.inputs) match {
      case errors: List[Throwable] if errors.nonEmpty =>
        handleError("extract", errors)
      case _ =>
        TransformSqlService(spark)(config.sql) match {
          case Failure(error) => handleError("transform", error)
          case Success(transformed) =>
            LoadDataframeService(transformed, config.output) match {
              case Some(error) => handleError("load", error)
              case None        => handleSuccess
            }
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
