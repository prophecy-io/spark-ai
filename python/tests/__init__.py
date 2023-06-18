import os
import unittest

from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv

class BaseUnitTest(unittest.TestCase):
    debug = False

    def setUp(self):
        load_dotenv()
        self.debug = os.getenv('DEBUG', 'False').lower() == 'true'

        self.spark = SparkSession.builder.master('local[1]').getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def df_debug(self, df: DataFrame):
        if self.debug:
            df.printSchema()
            df.show(truncate=False)
