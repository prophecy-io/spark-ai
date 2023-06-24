package io.prophecy.spark_ai.webapps.slack

import io.prophecy.spark_ai.utils.JsonUtilities
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.json4s.JsonAST

import scala.util.Try

class SlackRestClient(appToken: String, token: Option[String] = None) {

  val client = new OkHttpClient()
  private val jsonType = MediaType.parse("application/json; charset=utf-8");

  def socketUrl(): Either[Throwable, String] = {
    val request = new Request.Builder()
      .url("https://slack.com/api/apps.connections.open")
      .post(RequestBody.create("", null))
      .header("Authorization", "Bearer " + appToken)
      .build()

    for {
      response <- Try(client.newCall(request).execute()).toEither
      content = response.body().string()
      url <- parseResponse(content)
    } yield url
  }

  def postMessage(channel: String, text: String): Either[Throwable, String] = {
    def request(token: String) =
      new Request.Builder()
        .url("https://slack.com/api/chat.postMessage")
        .post(RequestBody.create(
          s"""
             |{
             |  "channel": "$channel",
             |  "text": "$text"
             |}
             |""".stripMargin, jsonType))
        .header("Authorization", "Bearer " + token)
        .build()

    for {
      token <- this.token.toRight(new Exception("No token provided"))
      response <- Try(client.newCall(request(token)).execute()).toEither
      content = response.body().string()
    } yield content
  }

  private def parseResponse(content: String): Either[Throwable, String] =
    JsonUtilities.parseJson(content).flatMap(parsed => parsed \\ "url" match {
      case JsonAST.JString(url) => Right(url)
      case _ => Left(new Exception(s"Slack returned unexpected response: `$content`"))
    })

}
