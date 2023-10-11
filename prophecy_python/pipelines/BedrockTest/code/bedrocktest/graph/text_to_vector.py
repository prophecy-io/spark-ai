from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bedrocktest.config.ConfigStore import *
from bedrocktest.udfs.UDFs import *

def text_to_vector(spark: SparkSession, empty_dataframe: DataFrame) -> DataFrame:
    return empty_dataframe.select(lit("Some text to vectorize").alias("text"))
