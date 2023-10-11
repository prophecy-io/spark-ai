from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class Bedrock(ComponentSpec):
    name: str = "Bedrock"
    category: str = "Machine Learning"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class BedrockProperties(ComponentProperties):
        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "aws"
        credential_db_key: Optional[str] = "key"
        credential_db_secret: Optional[str] = "secret"
        credential_manual_key: Optional[str] = ""
        credential_manual_secret: Optional[str] = ""
        # Potential values: embed_texts
        operation: str = "embed_texts"
        embed_text_column_name: Optional[str] = None
        # Model properties
        aws_region: Optional[str] = "us-east-1"

    def dialog(self) -> Dialog:
        def iff(property_name: str, value, then: Atom) -> Condition:
            value_expr = BooleanExpr(value) if isinstance(value, bool) else StringExpr(str(value))
            return Condition().ifEqual(PropExpr(f"component.properties.{property_name}"), value_expr).then(then)
            
        credential_type = RadioGroup("Credentials") \
            .addOption("Databricks Secrets (recommended)", "databricks") \
            .addOption("Hardcoded", "manual") \
            .bindProperty("credential_type")
        credential_db = ColumnsLayout(gap="1rem") \
            .addElement(TextBox("Databricks Scope").bindProperty("credential_db_scope")) \
            .addElement(TextBox("Databricks Secret Key Name").bindProperty("credential_db_key")) \
            .addElement(TextBox("Databricks Secret Value Name").bindProperty("credential_db_secret"))
        credential_manual = ColumnsLayout(gap="1rem") \
            .addElement(TextBox("AWS Access Key Id").bindPlaceholder("AWS Access Key Id").bindProperty("credential_manual_key")) \
            .addElement(TextBox("AWS Access Secret Key").bindPlaceholder("AWS Access Secret Key").bindProperty("credential_manual_secret"))
        credential_db_or_manual = Condition() \
            .ifEqual(PropExpr("component.properties.credential_type"), StringExpr("databricks")) \
            .then(credential_db).otherwise(credential_manual)
        credential = StackLayout() \
            .addElement(credential_type) \
            .addElement(credential_db_or_manual)
        operation_selector = SelectBox("Operation type") \
            .addOption("Compute text embeddings", "embed_texts") \
            .bindProperty("operation")

        # Text embedding properties
        embed_text_column_selector = SchemaColumnsDropdown("Text column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("embed_text_column_name") \
            .showErrorsFor("embed_text_column_name")

        # Model container
        aws_region = SelectBox("AWS Region") \
            .addOption("us-east-1 - US East (N. Virginia)", "us-east-1") \
            .addOption("us-west-2 - US West (Oregon)", "us-west-2") \
            .addOption("ap-southeast-1 - Asia Pacific (Singapore)", "ap-southeast-1") \
            .addOption("ap-northeast-1 - Asia Pacific (Tokyo)", "ap-northeast-1") \
            .bindProperty("aws_region")

        model_container = StackLayout(gap="1rem") \
            .addElement(TitleElement("Model Properties")) \
            .addElement(aws_region)

        # Main container
        operation_container = StackLayout(gap="1rem") \
            .addElement(TitleElement("Operation")) \
            .addElement(operation_selector) \
            .addElement(iff("operation", "embed_texts", embed_text_column_selector))

        main_container = StackLayout(padding="1rem", gap="2rem") \
            .addElement(credential) \
            .addElement(operation_container) \
            .addElement(model_container)
        main_container.padding = "1rem"
        main_container.gap = "3rem"
        return Dialog("OpenAI").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs().importSchema(), "2fr")
            .addColumn(main_container, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[BedrockProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[BedrockProperties], newState: Component[BedrockProperties]) -> Component[
    BedrockProperties]:
        return newState


    class BedrockCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Bedrock.BedrockProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from spark_ai.llms.bedrock import BedrockLLM
            from pyspark.sql.types import StringType

            access_key = ""
            access_secret = ""
            if self.props.credential_type == "databricks":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                access_key = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_key)
                access_secret = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_secret)
            else:
                access_key = self.props.credential_manual_key
                access_secret = self.props.credential_manual_secret

            (BedrockLLM(
                aws_access_key_id=access_key,
                aws_secret_access_key=access_secret,
                region_name=self.props.aws_region)
             .register_udfs(spark=self.spark))

            return in0 \
                .withColumn("_texts", array(col(self.props.embed_text_column_name))) \
                .withColumn("_embedded", expr("bedrock_embed_texts(_texts)")) \
                .withColumn("bedrock_embedding", expr("_embedded.embeddings[0]")) \
                .withColumn("bedrock_error", col("_embedded.error")) \
                .drop("_texts", "_embedded")

