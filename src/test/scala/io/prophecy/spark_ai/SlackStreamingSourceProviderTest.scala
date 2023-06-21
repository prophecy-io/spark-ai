package io.prophecy.spark_ai

import io.github.cdimascio.dotenv.Dotenv
import io.prophecy.spark_ai.webapps.slack.SlackSocketClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

class SlackStreamingSourceProviderTest extends AnyFunSuiteLike {

  private val dotenv = Dotenv.load()
  private val token = dotenv.get("SLACK_APP_TOKEN")

  test("Receiver") {
    val client = SlackSocketClient.initFromToken(token) match {
      case Right(client) => client
      case Left(error) => throw error
    }

    client.start()
    Thread.sleep(10000)
    client.shutdown()
  }

  test("Hello, world!") {
    val spark = SparkSession.builder()
      .appName("Spark Stream")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val dfMessages = spark.readStream.format("io_prophecy.spark_ai.webapps.slack.SlackSourceProvider")
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
