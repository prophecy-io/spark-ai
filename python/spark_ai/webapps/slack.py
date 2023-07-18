import json
import math
import os
import time
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col, max as sql_max
from pyspark.sql.types import *
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackUtilities:

    def __init__(self, token: str, spark: SparkSession, path_tmp: str = 'dbfs:/tmp/slack_data/', limit: int = -1):

        self.token = token
        self.client = WebClient(token=token)

        self.spark = spark

        self.path_tmp = path_tmp
        self.limit = limit

    def read_channels(self) -> DataFrame:
        os.makedirs(self.path_tmp, exist_ok=True)
        path_channels = os.path.join(self.path_tmp, 'channels.json')

        cursor = None
        channels = []

        while True:
            result = self.client.conversations_list(cursor=cursor,
                                                    limit=min(self.limit if self.limit != -1 else math.inf, 100))
            cursor = result.data.get('response_metadata', {}).get('next_cursor')

            for channel in result['channels']:
                channels.append(channel)

            if not cursor:
                break

            if self.limit != -1 and len(channels) > self.limit:
                break

        df_channels_text = self.spark.createDataFrame([Row(json.dumps(channel)) for channel in channels])
        df_channels_text.write.mode('overwrite').text(path_channels)

        return self.spark.read.json(path_channels)

    def join_channels(self, df_channels: DataFrame) -> DataFrame:
        client = self.client

        @udf(returnType=StructType([
            StructField("ok", BooleanType()),
            StructField("channel", StructType([
                StructField("id", StringType())
            ])),
            StructField("warning", StringType()),
            StructField("error", StringType())
        ]))
        def udf_join_channels(channel_id):
            time.sleep(1)

            try:
                return client.conversations_join(channel=channel_id).data
            except SlackApiError as error:
                return error.response

        return df_channels \
            .repartition(2) \
            .withColumn('result', udf_join_channels(col('id')))

    @staticmethod
    def find_max_ts_per_channel(df_conversations: DataFrame) -> Dict[str, float]:
        results = df_conversations \
            .groupBy(col('channel_id')) \
            .agg(sql_max(col('ts')).alias('max_ts')) \
            .select('channel_id', 'max_ts') \
            .collect()

        max_ts_per_channel = {}
        for row in results:
            max_ts_per_channel[row['channel_id']] = row['max_ts']

        return max_ts_per_channel

    def read_conversations(self, df_channels: DataFrame, max_ts_per_channel=None) -> DataFrame:
        # ------------------------------------------------
        # Fetches the already last timestamps for channels
        # ------------------------------------------------

        if max_ts_per_channel is None:
            max_ts_per_channel = {}

        # ------------------------------------------------
        # Fetches the latest conversations
        # ------------------------------------------------

        full_conversation_history_size = 0
        conversation_history_batch = []
        batch_idx = 0

        path_conversations = os.path.join(self.path_tmp, f'conversations/')

        if self.limit == -1:
            channels = df_channels.collect()
        else:
            channels = df_channels.limit(self.limit).collect()

        for idx, channel in enumerate(channels):
            channel_id = channel['id']

            if channel['is_member'] is not True:
                continue

            # Call the conversations.history method using the WebClient
            # conversations.history returns the first 100 messages by default
            # These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination

            cursor = None
            while True:
                result = self._get_conversations_history(channel_id, cursor=cursor,
                                                         limit=min(self.limit if self.limit != -1 else math.inf,
                                                                   10 * 1000),
                                                         oldest=max_ts_per_channel.get(channel_id, "0"))
                cursor = result.data.get('response_metadata', {}).get('next_cursor')

                for message in result["messages"]:
                    message['channel_id'] = channel_id
                    conversation_history_batch.append(message)

                    if 'thread_ts' in message and message['thread_ts'] is not None:
                        for thread_message in self._get_all_conversations_replies(channel_id, message['thread_ts']):
                            conversation_history_batch.append(thread_message)

                    if len(conversation_history_batch) > (self.limit if self.limit != -1 else math.inf):
                        break

                print(
                    f"Progress: {idx} out of {len(channels)} channels and {full_conversation_history_size + len(conversation_history_batch)} messages total")

                if len(conversation_history_batch) > min(self.limit if self.limit != -1 else math.inf, 10 * 1000):
                    df_batch = self.spark.createDataFrame(
                        [Row(json.dumps(message)) for message in conversation_history_batch])
                    mode = 'overwrite' if batch_idx == 0 else 'append'
                    df_batch.write.mode(mode).text(path_conversations)

                    full_conversation_history_size += len(conversation_history_batch)

                    conversation_history_batch = []
                    batch_idx += 1

                if not cursor:
                    break

                if full_conversation_history_size > self.limit:
                    break

            if full_conversation_history_size > self.limit:
                break

        if len(conversation_history_batch) > 0:
            df_batch = self.spark.createDataFrame([Row(json.dumps(message)) for message in conversation_history_batch])
            mode = 'overwrite' if batch_idx == 0 else 'append'
            df_batch.write.mode(mode).text(path_conversations)

        return self.spark.read.json(path_conversations)

    def _list_to_df(self, elements):
        R = Row('data')
        rows = [R(json.dumps(element)) for element in elements]
        return self.spark.createDataFrame(rows)

    def _any_to_df(self, obj):
        R = Row('data')
        rows = [R(json.dumps(obj))]
        return self.spark.createDataFrame(rows)

    def _get_conversations_history(self, channel_id, cursor=None, limit=500, max_retries=5, oldest="0"):
        retries = 0
        while retries <= max_retries:
            try:
                response = self.client.conversations_history(channel=channel_id, cursor=cursor, limit=limit,
                                                             oldest=oldest)
                return response
            except SlackApiError as e:
                if 'error' in e.response and e.response["error"] == "ratelimited":
                    retries += 1
                    time.sleep(5)  # wait for 5 seconds before retrying
                else:
                    raise e  # re-raise the exception if it's not a rate limit error
        raise Exception("Reached maximum number of retries")

    def _get_conversations_replies(self, channel_id, thread_ts, cursor=None, limit=500, max_retries=5):
        retries = 0
        while retries <= max_retries:
            try:
                response = self.client.conversations_replies(channel=channel_id, ts=thread_ts, cursor=cursor,
                                                             limit=limit)
                return response
            except SlackApiError as e:
                if 'error' in e.response and e.response["error"] == "ratelimited":
                    retries += 1
                    time.sleep(5)  # wait for 5 seconds before retrying
                else:
                    raise e  # re-raise the exception if it's not a rate limit error
        raise Exception("Reached maximum number of retries")

    def _get_all_conversations_replies(self, channel_id, thread_ts):
        messages = []
        cursor = None
        while True:
            result = self._get_conversations_replies(channel_id, thread_ts, cursor=cursor)
            for message in result['messages']:
                message['channel_id'] = channel_id
                messages.append(message)

            if not cursor:
                break

        return messages

    def write_messages(self, df: DataFrame):
        if not df.isStreaming:
            raise TypeError("Slack messages write is for streaming pipelines only")

        df.writeStream.outputMode("update").foreachBatch(self._write_batch).start()

    def _write_batch(self, df_batch: DataFrame, epoch_id: int):
        responses = df_batch.collect()
        for response in responses:
            client = WebClient(token=self.token)

            try:
                client.chat_postMessage(
                    channel=response["channel"],
                    text=response["answer"],
                    thread_ts=response["ts"]
                )
            except SlackApiError as e:
                assert e.response["ok"] is False
                assert e.response["error"]
                print(f"Got an error: {e.response}")
