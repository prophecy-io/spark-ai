from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType


class FileTextUtils:

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('text_split_into_chunks', self.split_into_chunks, returnType=ArrayType(StringType()))

    @staticmethod
    def split_into_chunks(text: str, chunk_size: int = 100) -> List[str]:
        lst = text.split(' ')
        return [' '.join(lst[i:i + chunk_size]) for i in range(0, len(lst), chunk_size)]
