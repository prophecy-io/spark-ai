from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from bedrocktest.config.ConfigStore import *
from bedrocktest.udfs.UDFs import *
from prophecy.utils import *
from bedrocktest.graph import *

def pipeline(spark: SparkSession) -> None:
    df_empty_dataframe = empty_dataframe(spark)
    df_text_to_vector = text_to_vector(spark, df_empty_dataframe)
    df_embed = embed(spark, df_text_to_vector)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/BedrockTest")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/BedrockTest", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/BedrockTest")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
