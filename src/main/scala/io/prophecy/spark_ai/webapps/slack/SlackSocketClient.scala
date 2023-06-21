package io.prophecy.spark_ai.webapps.slack

import io.prophecy.spark_ai.webapps.slack.SlackMessages.{SlackEvent, SlackResponse}
import SlackMessages.{SlackAcknowledge, SlackEvent, SlackHello, SlackResponse}

import java.net.URI
import java.net.http.{HttpClient, WebSocket}
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.mutable

object SlackSocketClient {
  def initFromToken(appToken: String): Either[Throwable, SlackSocketClient] = {
    new SlackRestClient(appToken).socketUrl().map(url => new SlackSocketClient(url))
  }
}


class SlackSocketClient(socketUrl: String) {

  private var socketThread: Option[Thread] = None
  private var webSocketClient: Option[CompletableFuture[WebSocket]] = None

  def start(): Unit = {
    val socketThread = new Thread("SlackStream") {
      setDaemon(true)

      override def run(): Unit = {
        val webSocketClient = HttpClient.newHttpClient().newWebSocketBuilder()
          .buildAsync(new URI(socketUrl), new WebSocketReceiver())

        SlackSocketClient.this.webSocketClient = Some(webSocketClient)
      }
    }

    socketThread.setUncaughtExceptionHandler((t, e) => {
      e.printStackTrace()
      throw e
    })

    socketThread.start()

    this.socketThread = Some(socketThread)
  }

  private val listeners: mutable.Map[String, SlackEvent => Unit] = mutable.Map()

  def addEventListener(listener: SlackEvent => Unit): () => Unit = {
    val id = UUID.randomUUID().toString
    listeners += id -> listener
    () => listeners -= id
  }

  private def onEvent(event: SlackEvent): Unit = {
    listeners.values.foreach(_.apply(event))
  }

  def shutdown(): Unit = {
    println("SlackSocketClient.onStop")

    this.webSocketClient.foreach(_.join().abort())
    this.webSocketClient.foreach(_.cancel(true))
    this.socketThread.foreach(_.interrupt())
  }

  private class WebSocketReceiver extends WebSocket.Listener {
    private val processedEventsChannelTs = new mutable.HashSet[(String, String)]()

    override def onOpen(webSocket: WebSocket): Unit = {
      super.onOpen(webSocket)
    }

    override def onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage[_] = {
      println(s"Got message: `$data`")

      SlackResponse.parse(data.toString) match {
        case Right(hello@SlackHello(_, _)) =>
          println(s"Got hello: $hello")
        case Right(event@SlackEvent(envelopeId, ts, channel, text, user, raw)) =>
          val key = (channel, ts)

          if (!processedEventsChannelTs.contains(key)) {
            processedEventsChannelTs.add(key)
            onEvent(event)
          } else {
            println(s"This event has already been processed ($key)")
          }

          webSocket.sendText(SlackAcknowledge(envelopeId).toString, true)
        case Left(error) =>
          println(s"Failed to decode message: ${error.toString}")
      }

      super.onText(webSocket, data, last)
    }

    override def onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage[_] = {
      super.onClose(webSocket, statusCode, reason)
    }

    override def onError(webSocket: WebSocket, error: Throwable): Unit = {
      error.printStackTrace()

      throw error
    }
  }

}
