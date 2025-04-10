from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType


class FileTextUtils:

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('text_split_into_chunks', self.split_into_chunks, returnType=ArrayType(StringType()))

    @staticmethod
    def split_into_chunks(text: str, chunk_size: int = 100, overlap_size=0) -> List[str]:
        lst = text.split(' ')
        chunks=[]
        #return [' '.join(lst[i:i + chunk_size]) for i in range(0, len(lst), chunk_size)]
        for i in range(0, len(lst), chunk_size-overlap_size):
            chunks.append(' '.join(lst[i:i + chunk_size]))
            if(i+chunk_size >= len(lst)):
                break
        return chunks

