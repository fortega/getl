package com.github.fortega.model

case class Config(
    inputs: List[ViewDefinition],
    output: Option[OutputDefinition],
    sql: String
)
