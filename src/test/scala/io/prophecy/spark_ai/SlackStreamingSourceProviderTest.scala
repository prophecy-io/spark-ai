package io.prophecy.spark_ai

import io.github.cdimascio.dotenv.Dotenv
import io.prophecy.spark_ai.webapps.slack.{SlackRestClient, SlackSocketClient}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuiteLike

import java.util.UUID

class SlackStreamingSourceProviderTest extends AnyFunSuiteLike {

  private val dotenv = Dotenv.load()
  private val appToken = dotenv.get("SLACK_APP_TOKEN")
  private val token = dotenv.get("SLACK_TOKEN")

  test("Test socket url") {
    val client = new SlackRestClient(appToken)
    client.socketUrl() match {
      case Right(url) => assert(url.startsWith("wss://wss-primary.slack.com/"))
      case Left(error) => throw error
    }
  }

  test("Hello, world!") {
    val spark = SparkSession.builder()
      .appName("Spark Stream")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val dfMessages = spark.readStream.format("io.prophecy.spark_ai.webapps.slack.SlackSourceProvider")
      .option("token", appToken)
      .load()

    val expectedContent = UUID.randomUUID().toString
    var found = false

    val stream = dfMessages.writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        batchDF.collect().foreach(row => {
          val content = row.getAs[String]("event")
          if (content.contains(expectedContent)) {
            found = true
          }
        })
      })
      .start()

    val restClient = new SlackRestClient(appToken, Some(token))
    restClient.postMessage("C05CM7QDRRV", s"Automated test: $expectedContent") match {
      case Right(_) => ()
      case Left(error) => throw error
    }

    Thread.sleep(2500)
    assert(found)
    stream.stop()
  }

}
