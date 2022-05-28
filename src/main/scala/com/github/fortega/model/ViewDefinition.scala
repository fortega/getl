package com.github.fortega.model

import org.apache.spark.sql.SparkSession

case class ViewDefinition(
    name: String,
    format: String,
    options: Map[String, String],
    schema: Option[String] = None
)
