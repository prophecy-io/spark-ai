import json
import os

from pyspark import Row
from pyspark.sql.functions import expr

from spark_ai.endpoints.sagemaker import SageMakerEndpoint
from tests import BaseUnitTest


class TestSageMakerEndpoint(BaseUnitTest):
    aws_access_key_id = ''
    aws_secret_access_key = ''
    aws_region = ''

    def setUp(self):
        super().setUp()

        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')

        (SageMakerEndpoint(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region)
         .register_udfs(spark=self.spark))

    def test_endpoint(self):
        data = [[
            json.dumps({
                'inputs': [[{'role': 'user', 'content': 'What is 2+2?'}]],
                'parameters': {'max_new_tokens': 512, 'top_p': 0.9, 'temperature': 0.6}
            }),
            '2+2 is 4'
        ]]

        r = Row('query', 'answer_part')
        df_data = self.spark.createDataFrame([r(query, answer) for query, answer in data])

        endpoint = 'meta-textgeneration-llama-2-7b-f-2023-10-09-19-36-35-366'
        attributes = 'accept_eula=true'

        df_answered = df_data\
            .withColumn('response', expr(f'sagemaker_invoke("{endpoint}", "{attributes}", query)'))\
            .withColumn('contains_answer', expr('contains(response.Body, answer_part)'))

        if self.debug:
            df_answered.show(truncate=False)

        self.assertEqual(df_answered.filter('contains_answer = true').count(), 1)
