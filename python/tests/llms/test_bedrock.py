import os

from pyspark import Row
from pyspark.sql.functions import expr, col

from spark_ai.llms.bedrock import BedrockLLM
from tests import BaseUnitTest


class TestBedrockLLM(BaseUnitTest):
    aws_access_key_id = ''
    aws_secret_access_key = ''
    aws_region = ''

    def setUp(self):
        super().setUp()

        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')

        (BedrockLLM(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region)
         .register_udfs(spark=self.spark))

    def test_embed_texts(self):
        data = [
            ['Hello, my dog is cute', 'Hello, my cat is cute'],
            ['Hello world', 'Hello Poland']
        ]

        r = Row('texts')
        df_data = self.spark.createDataFrame([r(text) for text in data])

        df_embedded = df_data \
            .withColumn('embedded', expr('bedrock_embed_texts(texts)')) \
            .select(col('texts').alias('texts'), col('embedded.embeddings').alias('embeddings'),
                    col('embedded.error').alias('error')) \
            .select(expr('explode_outer(arrays_zip(texts, embeddings))').alias('content'), col('error')) \
            .select(col('content.texts').alias('text'), col('content.embeddings').alias('embedding'), col('error')) \
            .withColumn('len', expr('size(embedding)'))

        if self.debug:
            df_embedded.show(truncate=False)

        self.assertEqual(df_embedded.filter('error is null').count(), 4)
        self.assertEqual(df_embedded.filter('size(embedding) = 1536').count(), 4)
