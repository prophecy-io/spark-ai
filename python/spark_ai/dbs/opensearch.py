from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from spark_ai.spark import SparkUtils

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import json

@dataclass
class UpsertResponse:
    count: int = 0
    error: Optional[str] = None

class OpensearchDB:

    def __init__(self, host:str, region:str, key: str, secret: str, service:str):
        self.host = host
        self.region = region
        self.service = service
        self.auth=(key,secret)
        credentials = boto3.Session(aws_access_key_id=key,aws_secret_access_key=secret).get_credentials()
        self.auth = AWSV4SignerAuth(credentials, region, service)

    def register_opensearch(self):
        client = OpenSearch(
            hosts=[{'host': self.host, 'port': 443}],
            http_auth=self.auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )
        return client

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('opensearch_upsert', self.upsert, returnType=self.upsert_type())

    def upsert(self, id_vectors) -> UpsertResponse:
        client = self.register_opensearch()

        try:
            count=0
            for id_vector in id_vectors:
                upsert_vectors=[]
                upsert_vectors.append(json.loads(id_vector))
                response = helpers.bulk(client, upsert_vectors)
                count=count + response[0]

            return UpsertResponse(count)

        except Exception as error:
            return UpsertResponse(error=str(error))
    @staticmethod
    def upsert_type() -> StructType:
        return SparkUtils.dataclass_to_spark(UpsertResponse)

