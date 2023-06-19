package io_prophecy.spark_ai.utils

import com.fasterxml.jackson.core.JacksonException
import org.json4s.{JObject, JValue, JsonAST, jackson}

import scala.util.{Failure, Success, Try}

object JsonUtilities {

  def parseJson(content: String): Either[JacksonException, JValue] =
    Try(jackson.parseJson(content)) match {
      case Success(value) => Right(value)
      case Failure(error: JacksonException) => Left(error)
      case Failure(uncheckedError) =>
        println(s"Unchecked error found: $uncheckedError")
        uncheckedError.printStackTrace()
        throw uncheckedError
    }

  def extractString(json: JValue, key: String): Either[Throwable, String] =
    json \ key match {
      case JsonAST.JString(value) => Right(value)
      case _ => Left(new Exception(s"Couldn't find a string $key at in $json"))
    }

  def extractInt(json: JValue, key: String): Either[Throwable, BigInt] =
    json \ key match {
      case JsonAST.JInt(value) => Right(value.toInt)
      case _ => Left(new Exception(s"Couldn't find an int $key at in $json"))
    }

  def extractObj(json: JValue, key: String): Either[Throwable, JObject] =
    json \ key match {
      case obj @ JsonAST.JObject(_) => Right(obj)
      case _ => Left(new Exception(s"Couldn't find an int $key at in $json"))
    }




}
