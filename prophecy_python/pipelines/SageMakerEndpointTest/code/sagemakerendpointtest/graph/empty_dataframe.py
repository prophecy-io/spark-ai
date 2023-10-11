from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sagemakerendpointtest.config.ConfigStore import *
from sagemakerendpointtest.udfs.UDFs import *

def empty_dataframe(spark: SparkSession) -> DataFrame:
    out0 = spark.range(1)

    return out0
