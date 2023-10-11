from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sagemakerendpointtest.config.ConfigStore import *
from sagemakerendpointtest.udfs.UDFs import *

def extract(spark: SparkSession, answer: DataFrame) -> DataFrame:
    return answer.select(col("question"), trim(col("sagemaker_answer.choices")[0]).alias("answer"))
