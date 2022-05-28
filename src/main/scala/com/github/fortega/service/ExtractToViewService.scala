package com.github.fortega.service

import com.github.fortega.model.ViewDefinition
import org.apache.spark.sql.SparkSession
import scala.util.Try
import scala.collection.GenTraversableOnce

object ExtractToViewService {
  def apply(spark: SparkSession): List[ViewDefinition] => List[Throwable] =
    _.flatMap { input =>
      try {
        val reader = spark.read
          .format(input.format)
          .options(input.options)
        val data = input.schema match {
          case None         => reader.load
          case Some(schema) => reader.schema(schema).load
        }
        data.createTempView(input.name)
        None
      } catch { case e: Throwable => Some(e) }
    }
}
