import os

from pyspark import Row
from pyspark.sql.functions import expr, col

from spark_ai.llms.sagemaker import SageMakerLLM
from tests import BaseUnitTest


class TestSageMakerLLM(BaseUnitTest):
    aws_access_key_id = ''
    aws_secret_access_key = ''
    aws_region = ''

    def setUp(self):
        super().setUp()

        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')

        (SageMakerLLM(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region)
         .register_udfs(spark=self.spark))

    def test_answer_question(self):
        data = [
            ['Barack Obama is the president of the United States.', 'Who is the president of the United States?',
             'Barack Obama'],
            ['Prophecy is a Low-code Data Engineering Platform.', 'What is Prophecy?', 'low-code'],
        ]

        r = Row('context', 'query', 'expected_answer')
        df_data = self.spark.createDataFrame([r(context, query, answer) for context, query, answer in data])

        endpoint = 'meta-textgeneration-llama-2-7b-f-2023-10-09-19-36-35-366'
        attributes = 'accept_eula=true'
        parameters = {'max_new_tokens': 512, 'top_p': 0.9, 'temperature': 0.6}
        default_prompt_qa = """Answer the question based on the context below.

Context:
```
{context}
```

Question: 
```
{query}
```

Answer:
    """

        df_answered = df_data \
            .withColumn('_parameters', expr(
            f'''named_struct("max_new_tokens", {parameters['max_new_tokens']}, "top_p", {parameters['top_p']}, "temperature", {parameters['temperature']})''')) \
            .withColumn('generated_answer',
                        expr(f'sagemaker_answer_question(context, query, "{default_prompt_qa}", "{endpoint}", _parameters, "{attributes}")')) \
            .select(col('*'), col('generated_answer.*')) \
            .withColumn('best_generated_answer', expr('choices[0]')) \
            .withColumn('answer_comparison', expr('contains(lower(best_generated_answer), lower(expected_answer))'))

        if self.debug:
            df_answered.show(truncate=False)

        self.assertEqual(df_answered.filter('answer_comparison = true').count(), 2)
