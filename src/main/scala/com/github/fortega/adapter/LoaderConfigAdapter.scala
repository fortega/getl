package com.github.fortega.adapter

import com.github.fortega.port.ConfigPort
import org.slf4j.LoggerFactory

object LoaderConfigAdapter {
  lazy val logger = LoggerFactory.getLogger("LoaderConfig")

  private def getFromEnv(
      env: Map[String, String],
      envKey: String = "CONFIG_BASE64"
  ): Option[String] =
    env.get(envKey)

  private def getFromArgs(cmdArgs: Array[String]): Option[String] = {
    val base64 = String.join("", cmdArgs: _*)
    if (base64.isEmpty) None else Some(base64)
  }

  def apply(
      env: Map[String, String],
      cmdArgs: Array[String]
  )(
      f: String => ConfigPort
  ): Option[ConfigPort] = (getFromEnv(env) match {
    case base64: Some[String] => base64
    case None                 => getFromArgs(cmdArgs)
  }).map(f)
}
