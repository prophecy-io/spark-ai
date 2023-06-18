package io_prophecy.spark_ai

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

class SlackStreamingSourceProviderTest extends AnyFunSuiteLike {

  val token = ""

  test("Receiver") {
    new WebSocketReceiver(token, _ => ()).start()
  }

  test("Hello, world!") {
    val spark = SparkSession.builder()
      .appName("Spark Stream")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val dfMessages = spark.readStream.format("io_prophecy.spark_ai.SlackStreamingSourceProvider")
      .option("token", token)
      .load()

    dfMessages.writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        println("Batch id: " + batchId)
        batchDF.show(truncate = false)
      })
      .start()
      .awaitTermination()
  }

}
