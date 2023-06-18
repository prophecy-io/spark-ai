package io_prophecy.spark_ai

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.{JsonMethods, compactJson, parseJson}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse, WebSocket}
import java.util
import java.util.Calendar
import java.util.concurrent.{CompletableFuture, CompletionStage}
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable.ListBuffer


class SlackStreamingSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SlackStreamingTable(options.get("token"))
  }

  override def shortName(): String = "slack"
}

class SlackStreamingTable(appToken: String) extends Table with SupportsRead {
  override def name(): String = "SlackStreamingTable"

  override def schema(): StructType = StructType(columns().map(c => StructField(c.name(), c.dataType())))

  override def columns(): Array[Column] = Array(Column.create("event", StringType))

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.MICRO_BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new SlackMicroBatchStream(appToken, checkpointLocation)
  }
}

import java.util.concurrent.atomic.AtomicBoolean


class SlackMicroBatchStream(appToken: String, checkpointLocation: String) extends MicroBatchStream with Logging {
  val numPartitions: Int = 1

  @GuardedBy("this")
  private[spark_ai] var socket: WebSocketReceiver = null

  @GuardedBy("this")
  private var readThread: Thread = null

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  private val batches = new ListBuffer[(UTF8String, Long)]

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  /** This method is only used for unit test */
  def getCurrentOffset(): LongOffset = synchronized {
    currentOffset.copy()
  }

  private def initialize(): Unit = synchronized {
    readThread = new Thread("SlackStream") {
      setDaemon(true)

      override def run(): Unit = {
        SlackMicroBatchStream.this.socket = new WebSocketReceiver(appToken, (message: String) => {
          SlackMicroBatchStream.this.synchronized {
            val newData = (
              UTF8String.fromString(message),
              DateTimeUtils.millisToMicros(Calendar.getInstance().getTimeInMillis)
            )
            currentOffset += 1
            batches.append(newData)
          }
        })

        SlackMicroBatchStream.this.socket.start()
      }
    }

    readThread.setUncaughtExceptionHandler((t, e) => {
      e.printStackTrace()
      throw e
    })

    readThread.start()
  }

  override def initialOffset(): Offset = LongOffset(-1L)

  override def latestOffset(): Offset = {
    currentOffset
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    val slices = Array.fill(numPartitions)(new ListBuffer[(UTF8String, Long)])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    slices.map(SlackInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[SlackInputPartition].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = {
          InternalRow(slice(currentIdx)._1, slice(currentIdx)._2)
        }

        override def close(): Unit = {}
      }
    }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = end.asInstanceOf[LongOffset]

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      throw new IllegalStateException(
        s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    if (socket != null) {
      socket.onStop()
      socket = null
    }
  }

  override def toString: String = s"SlackMicroBatchStream"
}

case class SlackInputPartition(slice: ListBuffer[(UTF8String, Long)]) extends InputPartition

case class SlackConnectionResponse(ok: Boolean, url: String, error: Option[String])

sealed trait SlackMessage

case class SlackConnectionInfo(appId: String)

case class SlackHello(`type`: String, numConnections: Int, connectionInfo: SlackConnectionInfo) extends SlackMessage

// {"envelope_id":"61faa16c-df1a-458a-8e2a-dc09cb040529","payload":{"token":"P0whLSmQTdZ66jC9bqQVRRDQ","team_id":"T2Q1N3CG5","context_team_id":"T2Q1N3CG5","context_enterprise_id":null,"api_app_id":"A05AU1GNY7P","event":{"client_msg_id":"397b275a-a03e-42ac-83d3-3f291ddf8b6e","type":"message","text":"q","user":"UM7SGTB2Q","ts":"1687055664.875959","blocks":[{"type":"rich_text","block_id":"n6w","elements":[{"type":"rich_text_section","elements":[{"type":"text","text":"q"}]}]}],"team":"T2Q1N3CG5","channel":"C05AGD7NCG7","event_ts":"1687055664.875959","channel_type":"channel"},"type":"event_callback","event_id":"Ev05CV22UJ4E","event_time":1687055664,"authorizations":[{"enterprise_id":null,"team_id":"T2Q1N3CG5","user_id":"U05AU1K4ELV","is_bot":true,"is_enterprise_install":false}],"is_ext_shared_channel":false,"event_context":"4-eyJldCI6Im1lc3NhZ2UiLCJ0aWQiOiJUMlExTjNDRzUiLCJhaWQiOiJBMDVBVTFHTlk3UCIsImNpZCI6IkMwNUFHRDdOQ0c3In0"},"type":"events_api","accepts_response_payload":false,"retry_attempt":0,"retry_reason":""}
case class SlackEvent(envelopeId: String, `type`: String) extends SlackMessage

case class SlackAcknowledge(envelopeId: String, payload: Option[String] = None) extends SlackMessage

class WebSocketReceiver(appToken: String, fn: String => Unit) {

  private var webSocket: Option[CompletableFuture[WebSocket]] = None

  def start(): Unit = {
    val request = HttpRequest
      .newBuilder(new URI("https://slack.com/api/apps.connections.open"))
      .header("Authorization", "Bearer " + appToken)
      .POST(HttpRequest.BodyPublishers.noBody())
      .build()

    val client = HttpClient.newBuilder().build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    val url = parseJson(response.body()).asInstanceOf[JObject].obj.toMap
      .getOrElse("url", throw new Exception("Url missing")).asInstanceOf[JString].s

    val webSocket = HttpClient.newHttpClient().newWebSocketBuilder()
      .buildAsync(new URI(url), new WebSocketClient(fn))

    while (true) {
      Thread.sleep(100)
    }
  }

  def onStop(): Unit = {
    println("WebSocketReceiver.onStop")
  }
}

class WebSocketClient(fn: String => Unit) extends WebSocket.Listener {
  override def onOpen(webSocket: WebSocket): Unit = {
    super.onOpen(webSocket)
  }

  override def onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage[_] = {
    println(s"Got message: `$data`")

    val dataParsed = JsonMethods.parse(data.toString)
    dataParsed match {
      case JsonAST.JObject(obj) =>
        val objMap = obj.toMap

        objMap.get("type") match {
          case Some(JString("hello")) =>
            super.onText(webSocket, data, last)
          case Some(JString("events_api")) =>
            val envelopeId = objMap.getOrElse("envelope_id", throw new RuntimeException("Missing envelope_id")).asInstanceOf[JString].s

            debugEvent(dataParsed)

            fn(data.toString)
            webSocket.request(2)
            val ack = JObject("envelope_id" -> envelopeId, "payload" -> dataParsed \\ "payload")
            val ackMessage = compactJson(ack)
            println(s"Sending ack message: `${ackMessage}`")
            webSocket.sendText(ackMessage, true)
          case None => println("No type")
            super.onText(webSocket, data, last)
        }
      case _ =>
        println(s"Failed to decode message: ${data.toString}")
        super.onText(webSocket, data, last)
    }
  }

  private def debugEvent(event: JValue): Unit = {
    val text = (event \ "payload" \ "event" \ "text").asInstanceOf[JString].s
    val ts =(event \ "payload" \ "event" \ "ts").asInstanceOf[JString].s

    println(s"Message at $ts: `$text`")
  }

  override def onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage[_] = {
    super.onClose(webSocket, statusCode, reason)
  }

  override def onError(webSocket: WebSocket, error: Throwable): Unit = {
    error.printStackTrace()

    throw error
  }
}