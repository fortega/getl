package com.github.fortega.model

case class OutputDefinition(
    mode: String = "overwrite",
    format: String = "parquet",
    options: Map[String, String]
)
