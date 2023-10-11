from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bedrocktest.config.ConfigStore import *
from bedrocktest.udfs.UDFs import *

def empty_dataframe(spark: SparkSession) -> DataFrame:
    out0 = spark.range(1)

    return out0
