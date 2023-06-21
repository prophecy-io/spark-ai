package io.prophecy.spark_ai.webapps.slack

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class SlackSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SlackStreamingTable(options.get("token"))
  }

  override def shortName(): String = "slack"

  private class SlackStreamingTable(appToken: String) extends Table with SupportsRead {
    override def name(): String = "SlackStreamingTable"

    override def schema(): StructType = StructType(StructField("event", StringType) :: Nil)

//    For Spark 3.4.0
//    override def columns(): Array[Column] = Array(Column.create("event", StringType))

    override def capabilities(): util.Set[TableCapability] = Set(TableCapability.MICRO_BATCH_READ).asJava

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
      override def readSchema(): StructType = schema()

      override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
        new SlackMicroBatchStream(appToken, checkpointLocation)
    }
  }

}
