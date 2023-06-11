import os

from pyspark.sql.functions import expr, col
from pyspark.sql.types import Row

from src.spark_ai.llms.openai import OpenAiLLM
from tests import BaseUnitTest


class TestOpenAiLLM(BaseUnitTest):
    api_key = ''

    def setUp(self):
        super().setUp()

        self.api_key = os.getenv('OPENAI_API_KEY')
        OpenAiLLM(api_key=self.api_key).register_udfs(spark=self.spark)

    def test_embed_texts(self):
        data = [
            ['Hello, my dog is cute', 'Hello, my cat is cute'],
            ['Hello world', 'Hello Poland']
        ]

        r = Row('texts')
        df_data = self.spark.createDataFrame([r(text) for text in data])

        df_embedded = df_data \
            .withColumn('embedded', expr('openai_embed_texts(texts)')) \
            .select(col('texts').alias('texts'), col('embedded.embeddings').alias('embeddings'),
                    col('embedded.error').alias('error')) \
            .select(expr('explode_outer(arrays_zip(texts, embeddings))').alias('content'), col('error')) \
            .select(col('content.texts').alias('text'), col('content.embeddings').alias('embedding'), col('error'))

        self.assertEqual(df_embedded.filter('error is null').count(), 4)
        self.assertEqual(df_embedded.filter('size(embedding) = 1536').count(), 4)

    def test_answer_questions(self):
        data = [
            ['Barack Obama is the president of the United States.', 'Who is the president of the United States?',
             'Barack Obama'],
            ['Prophecy is a Low-code Data Engineering Platform.', 'What is Prophecy?', 'low-code'],
        ]

        r = Row('context', 'query', 'expected_answer')
        df_data = self.spark.createDataFrame([r(context, query, answer) for context, query, answer in data])

        df_answered = df_data \
            .withColumn('generated_answer', expr('openai_answer_question(context, query)')) \
            .select(col('*'), col('generated_answer.*')) \
            .withColumn('best_generated_answer', expr('choices[0]')) \
            .withColumn('answer_comparison', expr('contains(lower(best_generated_answer), lower(expected_answer))'))

        if self.debug:
            df_answered.show(truncate=False)

        self.assertEqual(df_answered.filter('answer_comparison = true').count(), 2)
