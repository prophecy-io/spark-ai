from pyspark.sql.functions import lit, expr

from spark_ai.webapps import WebUtils
from tests import BaseUnitTest


class TestWeb(BaseUnitTest):

    def _init_web(self) -> WebUtils:
        utils = WebUtils()
        utils.register_udfs(spark=self.spark)

        return utils

    def test_scrape(self):
        self._init_web()

        df_url = self.spark.range(1).select(lit("https://docs.prophecy.io/sitemap.xml").alias("url"))
        df_results = df_url.withColumn("content", expr("cast(web_scrape(url) as string)"))

        self.assertTrue(df_results.collect()[0].content.startswith("<?xml version="))
