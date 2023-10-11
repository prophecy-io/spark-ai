import decimal
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, Row

from spark_ai.endpoints.sagemaker import SageMakerEndpoint


class SageMakerLLM(SageMakerEndpoint):
    def register_udfs(self, spark: SparkSession):
        super().register_udfs(spark)

        spark.udf.register('sagemaker_answer_question', self.answer_question, returnType=self.answer_question_type())

    def answer_question(self, context: str, query: str, template: str, endpoint: str, parameters: Row,
                        attributes: str = ''):
        query = json.dumps({
            'inputs': [[{'role': 'user', 'content': template.format(context=context, query=query)}]],
            'parameters': parameters.asDict()
        }, cls=self.DecimalEncoder)
        print('query: ', query)

        response = self.invoke_json(endpoint_name=endpoint, attributes=attributes, body=query)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return {'choices': None, 'error': response['Body']}
        else:
            return {'choices': [json.loads(response['Body'])[0]['generation']['content']], 'error': None}

    @staticmethod
    def answer_question_type() -> StructType:
        return StructType([
            StructField('choices', ArrayType(StringType())),
            StructField('error', StringType())
        ])

    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                return float(o)
            return super().default(o)
