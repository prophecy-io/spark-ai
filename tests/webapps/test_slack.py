import os
import unittest

from pyspark.sql import SparkSession

from src.spark_ai.webapps.slack import SlackUtilities
from tests import BaseUnitTest


class TestSlackUtilities(BaseUnitTest):
    _token = None
    _path_tmp = '/tmp/slack_data/'
    _limit = 100

    def _init_slack(self, limit: int = _limit) -> SlackUtilities:
        return SlackUtilities(
            token=self._token,
            spark=self.spark,
            path_tmp=self._path_tmp,
            limit=limit
        )

    def setUp(self):
        super().setUp()

        self.spark = SparkSession.builder.master('local[1]').getOrCreate()
        self._token = os.getenv('SLACK_TOKEN')

    def test_read_channels(self):
        slack = self._init_slack()
        df_channels = slack.read_channels()

        self.assertIn('id', df_channels.columns)
        self.assertIn('name', df_channels.columns)

        self.assertTrue(df_channels.count() >= self._limit)

    def test_join_channels(self):
        slack = self._init_slack(limit=1)

        df_channels = slack.read_channels()
        df_results = slack.join_channels(df_channels)

        self.assertIn('id', df_results.columns)
        self.assertIn('result', df_results.columns)

        self.assertTrue(df_results.count() >= 1)

    def test_read_conversations(self):
        slack = self._init_slack()

        df_conversations = slack.read_conversations(df_channels=slack.read_channels())
        self.assertIn('text', df_conversations.columns)
        self.assertIn('ts', df_conversations.columns)
        self.assertIn('channel_id', df_conversations.columns)

        self.assertTrue(df_conversations.count() >= self._limit)

    def test_find_max_ts_per_channel(self):
        slack = self._init_slack()

        df_channels = slack.read_channels()
        df_conversations = slack.read_conversations(df_channels=df_channels)
        max_ts_per_channel = slack.find_max_ts_per_channel(df_conversations)

        self.assertTrue(len(max_ts_per_channel) >= 1)


if __name__ == '__main__':
    unittest.main()
