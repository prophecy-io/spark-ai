package io.prophecy.spark_ai.webapps.slack

import io.prophecy.spark_ai.utils.JsonUtilities
import JsonUtilities.{extractInt, extractObj, extractString}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.compact
import org.json4s.{JObject, JString, JsonAST}

object SlackMessages {

  sealed trait SlackMessage {
    def raw: JValue
  }

  object SlackResponse {
    def parse(message: String): Either[Throwable, SlackResponse] =
      JsonUtilities.parseJson(message).flatMap(parsed => {
        parsed \ "type" match {
          case JsonAST.JString("hello") => SlackHello.parse(parsed)
          case JsonAST.JString("events_api") => SlackEvent.parse(parsed)
          case _ => Left(new Throwable(s"Unrecognized message content: `$message`"))
        }
      })
  }

  sealed trait SlackResponse {
    def `type`: String
  }

  object SlackHello {
    def parse(value: JValue): Either[Throwable, SlackHello] =
      extractInt(value, "num_connections") match {
        case Right(numConnections) => Right(SlackHello(numConnections, value))
        case Left(error) => Left(new Exception(s"Couldn't parse hello message from the Slack's socket: ${compact(value)}", error))
      }
  }

  case class SlackHello(numConnections: BigInt, raw: JValue) extends SlackResponse {
    def `type`: String = "hello"
  }

  object SlackEvent {
    def parse(value: JValue): Either[Throwable, SlackEvent] = {
      val result = for {
        envelopeId <- extractString(value, "envelope_id")
        payload <- extractObj(value, "payload")
        event <- extractObj(payload, "event")
        text <- extractString(event, "text")
        ts <- extractString(event, "ts")
        channel <- extractString(event, "channel")
        user <- extractString(event, "user")
      } yield SlackEvent(envelopeId, ts, channel, text, user, value)

      result.left.map(error => new Exception(s"Couldn't parse event message from the Slack's socket: ${compact(value)}", error))
    }
  }

  case class SlackEvent(envelopeId: String, ts: String, channel: String, text: String, user: String, raw: JValue) extends SlackResponse {
    def `type`: String = "events_api"
  }

  sealed trait SlackRequest {
    def toJson: JValue

    override def toString: String = compact(toJson)
  }

  case class SlackAcknowledge(envelopeId: String, payload: Option[JValue] = None) extends SlackRequest {
    def toJson: JValue = {
      JObject(
        "envelope_id" -> JString(envelopeId),
        "payload" -> payload.getOrElse(JObject())
      )
    }
  }

}
