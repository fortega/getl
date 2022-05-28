package com.github.fortega.adapter

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fortega.port.ConfigPort
import com.github.fortega.model.Config
import scala.util.Try
import java.util.Base64
import scala.util.Failure

object JsonConfigAdapter {
  def apply(cmdArgs: Array[String]): ConfigPort = apply(
    String.valueOf(cmdArgs.flatten)
  )

  def apply(
      env: Map[String, String],
      key: String = "CONFIG"
  ): ConfigPort = env.get(key) match {
    case None         => Failure(new Exception(s"env $key not found"))
    case Some(base64) => apply(base64)
  }

  def apply(
      base64: String
  ): ConfigPort = Try {
    val mapper = JsonMapper.builder
      .addModule(DefaultScalaModule)
      .build
    mapper.readValue(
      Base64.getDecoder.decode(base64),
      classOf[Config]
    )
  }
}
