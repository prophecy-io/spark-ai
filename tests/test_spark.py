import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, struct, lit, explode, explode_outer
from pyspark.sql.types import StructField, StringType, ArrayType, StructType, IntegerType, FloatType

from src.spark_ai.dbs.pinecone import UpsertResponse, QueryResponse
from src.spark_ai.spark import SparkUtils
from tests import BaseUnitTest


class TestSparkUtils(BaseUnitTest):

    def test_default_missing_columns(self):
        df_original = self.spark.range(1).select(
            lit(1).alias('a'),
            struct(
                lit('c').alias('c'),
                array(
                    struct(lit('e').alias('e'), lit(None).cast(StringType()).alias('f')),
                    struct(lit('e').alias('e'), lit('f').alias('f'))
                ).alias('d')
            ).alias('b')
        )

        df_original.show(truncate=False)

        expected_schema = StructType([
            StructField('a', IntegerType(), False),
            StructField('b', StructType([
                StructField('c', StringType(), False),
                StructField('d', ArrayType(StructType([
                    StructField('e', StringType(), False),
                    StructField('f', StringType(), True),
                    StructField('g', StringType(), True)
                ]), containsNull=False), False),
                StructField('h', StringType(), True),
            ]), False)
        ])

        df_result = SparkUtils.default_missing_columns(df_original, expected_schema)
        self.assertEquals(expected_schema, df_result.schema)

    def test_dataclass_to_spark(self):
        upsert_response = SparkUtils.dataclass_to_spark(UpsertResponse)
        self.assertEquals(
            upsert_response,
            StructType([
                StructField('count', IntegerType(), False),
                StructField('error', StringType(), True)
            ])
        )

        query_response = SparkUtils.dataclass_to_spark(QueryResponse)
        self.assertEquals(
            query_response,
            StructType([
                StructField('matches', ArrayType(StructType([
                    StructField('id', StringType(), False),
                    StructField('score', FloatType(), False),
                ]), False), False),
                StructField('error', StringType(), True)
            ])
        )
