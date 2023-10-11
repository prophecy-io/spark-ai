from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sagemakerendpointtest.config.ConfigStore import *
from sagemakerendpointtest.udfs.UDFs import *

def answer(spark: SparkSession, text_to_vector: DataFrame) -> DataFrame:
    from spark_ai.llms.sagemaker import SageMakerLLM
    from pyspark.sql.types import StringType
    from pyspark.dbutils import DBUtils
    (SageMakerLLM(
          aws_access_key_id = DBUtils(spark).secrets.get(scope = "aws_main", key = "secret_key"),
          aws_secret_access_key = DBUtils(spark).secrets.get(scope = "aws_main", key = "secret_value"),
          region_name = "eu-west-1"
        )\
        .register_udfs(spark = spark))

    return text_to_vector\
        .withColumn("_context", col("context"))\
        .withColumn("_query", col("question"))\
        .withColumn(
          "_template",
          expr(
            " \" Answer the question based on the context below.\nContext:\n```\n{context}\n```\nQuestion: \n```\n{query}\n```\nAnswer:\n \" "
          )
        )\
        .withColumn("_endpoint", lit("meta-textgeneration-llama-2-7b-f-2023-10-09-19-36-35-366"))\
        .withColumn("_parameters", expr("named_struct('max_new_tokens', 512, 'top_p', 0.9, 'temperature', 0.6)"))\
        .withColumn("_attributes", lit("accept_eula=true"))\
        .withColumn(
          "sagemaker_answer",
          expr("""sagemaker_answer_question(_context, _query, _template, _endpoint, _parameters, _attributes)""")
        )\
        .drop("_context", "_query", "_template", "_endpoint", "_parameters", "_attributes")
