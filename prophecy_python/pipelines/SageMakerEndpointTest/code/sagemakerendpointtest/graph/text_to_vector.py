from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sagemakerendpointtest.config.ConfigStore import *
from sagemakerendpointtest.udfs.UDFs import *

def text_to_vector(spark: SparkSession, empty_dataframe: DataFrame) -> DataFrame:
    return empty_dataframe.select(
        lit("Who is the president of United States?").alias("question"), 
        lit(
            "Barack Obama was the previous president of the United States. Donald Trump was the current president before Joe Biden took office."
          )\
          .alias("context")
    )
