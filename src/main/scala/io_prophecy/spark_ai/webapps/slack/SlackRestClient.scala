package io_prophecy.spark_ai.webapps.slack

import io_prophecy.spark_ai.utils.JsonUtilities
import org.json4s.{JObject, JString, JsonAST}
import org.json4s.jackson.parseJson

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.util.Try

class SlackRestClient(appToken: String) {

  def socketUrl(): Either[Throwable, String] = {
    val request = HttpRequest
      .newBuilder(new URI("https://slack.com/api/apps.connections.open"))
      .header("Authorization", "Bearer " + appToken)
      .POST(HttpRequest.BodyPublishers.noBody())
      .build()

    val client = HttpClient.newBuilder().build()

    for {
      response <- Try(client.send(request, HttpResponse.BodyHandlers.ofString())).toEither
      content = response.body()
      url <- parseResponse(content)
    } yield url
  }

  private def parseResponse(content: String): Either[Throwable, String] =
    JsonUtilities.parseJson(content).flatMap(parsed => parsed \\ "url" match {
      case JsonAST.JString(url) => Right(url)
      case _ => Left(new Exception(s"Slack returned unexpected response: `$content`"))
    })

}
