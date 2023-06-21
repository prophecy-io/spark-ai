from dataclasses import dataclass, field
from typing import List, Optional

import pinecone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from spark_ai.spark import SparkUtils


@dataclass
class IdVector:
    id: str
    vector: List[float]


@dataclass
class UpsertResponse:
    count: int = 0
    error: Optional[str] = None


@dataclass
class QueryMatch:
    id: str
    score: float


@dataclass
class QueryResponse:
    matches: List[QueryMatch] = field(default_factory=lambda: [])
    error: Optional[str] = None


class PineconeDB:

    def __init__(self, api_key: str, environment: str):
        self.api_key = api_key
        self.environment = environment

    def register_pinecone(self):
        pinecone.init(api_key=self.api_key, environment=self.environment)

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('pinecone_upsert', self.upsert, returnType=self.upsert_type())
        spark.udf.register('pinecone_query', self.query, returnType=self.query_type())

    def upsert(self, index_name: str, id_vectors: List[IdVector]) -> UpsertResponse:
        self.register_pinecone()

        try:
            index = pinecone.Index(index_name)
            response = index.upsert([pinecone.Vector(id_vector.id, id_vector.vector) for id_vector in id_vectors])
            return UpsertResponse(count=response['upserted_count'])
        except Exception as error:
            return UpsertResponse(error=str(error))

    @staticmethod
    def upsert_type() -> StructType:
        return SparkUtils.dataclass_to_spark(UpsertResponse)

    def query(self, index_name: str, vector: List[float], top_k: int = 3) -> QueryResponse:
        self.register_pinecone()

        try:
            index = pinecone.Index(index_name)
            response = index.query(vector=vector, top_k=top_k, include_values=False)
            matches = [QueryMatch(id=match['id'], score=match['score']) for match in response.to_dict()['matches']]
            return QueryResponse(matches=matches)
        except Exception as error:
            return QueryResponse(error=str(error))

    @staticmethod
    def query_type() -> StructType:
        return SparkUtils.dataclass_to_spark(QueryResponse)
