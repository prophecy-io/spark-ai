package io.prophecy.spark_ai.webapps.slack

import io.prophecy.spark_ai.webapps.slack.SlackMessages.{SlackAcknowledge, SlackEvent, SlackHello, SlackResponse}
import okhttp3._

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable

object SlackSocketClient {
  def initFromToken(appToken: String): Either[Throwable, SlackSocketClient] = {
    new SlackRestClient(appToken).socketUrl().map(url => new SlackSocketClient(url))
  }
}


class SlackSocketClient(socketUrl: String) {

  private var socketThread: Option[Thread] = None

  private var httpClient: Option[OkHttpClient] = None
  private var webSocketClient: Option[WebSocket] = None

  def start(): Unit = {
    val socketThread = new Thread("SlackStream") {
      setDaemon(true)

      override def run(): Unit = {
        val client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build()
        val request = new Request.Builder().url(socketUrl).build()
        val websocket = client.newWebSocket(request, new WebSocketReceiver())

        httpClient = Some(client)
        webSocketClient = Some(websocket)

        // while (!Thread.interrupted()) {
        //   Thread.sleep(1000)
        //   onEvent(SlackEvent("heartbeat", null, null, null, null, null))
        // }
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

    this.webSocketClient.foreach(_.cancel())
    this.httpClient.foreach(_.dispatcher().executorService().shutdown())
    this.socketThread.foreach(_.interrupt())
  }

  private class WebSocketReceiver extends WebSocketListener {
    private val processedEventsChannelTs = new mutable.HashSet[(String, String)]()

    override def onOpen(webSocket: WebSocket, response: Response): Unit = {
      super.onOpen(webSocket, response)
    }

    override def onMessage(webSocket: WebSocket, text: String): Unit = {
      println(s"Got message: `$text`")

      SlackResponse.parse(text) match {
        case Right(hello@SlackHello(_, _)) =>
          println(s"Got hello: $hello")
        case Right(event@SlackEvent(envelopeId, ts, channel, _, _, _)) =>
          val key = (channel, ts)

          if (!processedEventsChannelTs.contains(key)) {
            processedEventsChannelTs.add(key)
            onEvent(event)
          } else {
            println(s"This event has already been processed ($key)")
          }

          webSocket.send(SlackAcknowledge(envelopeId).toString)
        case Left(error) =>
          println(s"Failed to decode message: ${error.toString}")
      }
    }

    override def onClosed(webSocket: WebSocket, code: Int, reason: String): Unit = {
      shutdown()
    }

    override def onFailure(webSocket: WebSocket, t: Throwable, response: Response): Unit = {
      t.printStackTrace()
      throw t
    }
  }

}
