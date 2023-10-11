import json
from typing import List

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, FloatType

from spark_ai import AWSService


class BedrockLLM(AWSService):
    default_embedding_model = 'amazon.titan-embed-text-v1'

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('bedrock_embed_texts', self.embed_texts, returnType=self.embed_texts_type())

    def embed_texts(self, texts: List[str], model: str = default_embedding_model):
        bedrock = boto3.client(
            'bedrock-runtime',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        try:
            embeddings = []
            for text in texts:
                body = json.dumps({"inputText": text})
                content_type = 'application/json'

                response = bedrock.invoke_model(body=body, modelId=model, accept=content_type, contentType=content_type)
                embeddings.append(json.loads(response['body'].read().decode('utf-8'))['embedding'])

            return {'embeddings': embeddings, 'error': None}
        except Exception as error:
            return {'embeddings': None, 'error': str(error)}

    @staticmethod
    def embed_texts_type():
        return StructType([
            StructField('embeddings', ArrayType(ArrayType(FloatType()))),
            StructField('error', StringType())
        ])
