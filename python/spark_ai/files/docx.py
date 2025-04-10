from io import BytesIO
from typing import List

from unstructured.partition.docx import partition_docx
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class FileDocxUtils:

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('docx_parse', self.parse, returnType=self.parse_type())

    @staticmethod
    def parse(binary: bytes) -> List[dict]:
        elements = partition_docx(file=BytesIO(binary))
        return [element.to_dict() for element in elements]

    @staticmethod
    def parse_type() -> DataType:
        return ArrayType(StructType([
            StructField("element_id", StringType()),
            StructField("coordinates", ArrayType(FloatType())),
            StructField("text", StringType()),
            StructField("type", StringType()),
            StructField("metadata", StructType([
                StructField("filename", StringType()),
                StructField("filetype", StringType()),
                StructField("page_number", IntegerType())
            ]))
        ]))
