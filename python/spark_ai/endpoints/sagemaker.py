from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from spark_ai import AWSService


class SageMakerEndpoint(AWSService):

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('sagemaker_invoke', self.invoke_json, returnType=self.invoke_json_type())

    def invoke_json(self, endpoint_name: str, attributes: str, body: str, content_type: str = 'application/json'):
        import boto3

        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        client = session.client('sagemaker-runtime')

        response = client.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=body,
            ContentType=content_type,
            CustomAttributes=attributes
        )

        response['Body'] = response['Body'].read().decode('utf-8')
        return response

    @staticmethod
    def invoke_json_type():
        return StructType([
            StructField('ResponseMetadata', StructType([
                StructField('RequestId', StringType()),
                StructField('HTTPStatusCode', IntegerType()),
            ])),
            StructField('ContentType', StringType()),
            StructField('InvokedProductionVariant', StringType()),
            StructField('Body', StringType())
        ])
