package io_prophecy.spark_ai.webapps.slack

import io_prophecy.spark_ai.webapps.slack.SlackMessages.SlackEvent
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.jackson.JsonMethods.compact

import java.util.Calendar
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable.ListBuffer

object SlackMicroBatchStream {

  private case class SlackInputPartition(slice: ListBuffer[(UTF8String, Long)]) extends InputPartition

}

class SlackMicroBatchStream(appToken: String, checkpointLocation: String) extends MicroBatchStream with Logging {

  import SlackMicroBatchStream._

  private val numPartitions: Int = 1

  @GuardedBy("this")
  private[spark_ai] var client: Option[SlackSocketClient] = None

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

  private var cancelEventListener: Option[() => Unit] = None

  private def initialize(): Unit = synchronized {
    val client = SlackSocketClient.initFromToken(appToken) match {
      case Right(client) => client
      case Left(error) => throw error
    }

    client.start()
    this.cancelEventListener = Some(client.addEventListener((event: SlackEvent) => synchronized {
      val newData = (
        UTF8String.fromString(compact(event.raw)),
        DateTimeUtils.millisToMicros(Calendar.getInstance().getTimeInMillis)
      )

      currentOffset += 1
      batches.append(newData)
    }))

    this.client = Some(client)
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
    cancelEventListener.foreach(_.apply())
    client.foreach(_.shutdown())
  }

  override def toString: String = s"SlackMicroBatchStream"
}