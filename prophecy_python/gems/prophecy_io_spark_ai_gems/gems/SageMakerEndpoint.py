from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class SageMakerEndpoint(ComponentSpec):
    name: str = "SageMakerEndpoint"
    category: str = "Machine Learning"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SageMakerEndpointProperties(ComponentProperties):
        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "aws"
        credential_db_key: Optional[str] = "key"
        credential_db_secret: Optional[str] = "secret"
        credential_manual_key: Optional[str] = ""
        credential_manual_secret: Optional[str] = ""
        # Potential values: embed_texts, chat_complete, answer_question
        operation: str = "answer_question"
        group_data: Optional[bool] = True
        order_by_column: Optional[str] = None
        embed_text_column_name: Optional[str] = None
        qa_context_column_name: Optional[str] = None
        qa_query_column_name: Optional[str] = None
        qa_template: Optional[str] = """Answer the question based on the context below.
Context:
```
{context}
```
Question: 
```
{query}
```
Answer:
"""
        limit: SInt = SInt("10")
        model_endpoint_name: Optional[str] = "meta-textgeneration-llama-2-7b-f-2023-10-09-19-36-35-366"
        model_max_new_tokens: Optional[int] = 512
        model_top_p: Optional[str] = "0.9"
        model_temperature: Optional[str] = "0.6"
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
            .addOption("Answer questions for given context", "answer_question") \
            .bindProperty("operation")
            # .addOption("Compute text embeddings", "embed_texts") \
            # .addOption("Answer questions", "chat_complete") \
        enable_order_column = Checkbox("Group data") \
            .bindProperty("group_data")
        order_column_selector = SchemaColumnsDropdown("Order by column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("order_by_column") \
            .showErrorsFor("order_by_column")
        # Text embedding properties
        embed_text_column_selector = SchemaColumnsDropdown("Texts column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("embed_text_column_name") \
            .showErrorsFor("embed_text_column_name")
        embed_column_selector = ColumnsLayout(gap="1rem") \
            .addElement(enable_order_column) \
            .addElement(iff("group_data", True, order_column_selector)) \
            .addElement(embed_text_column_selector)
        # Question answering properties
        qa_context_column_selector = SchemaColumnsDropdown("Context text column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("qa_context_column_name") \
            .showErrorsFor("qa_context_column_name")
        qa_query_column_selector = SchemaColumnsDropdown("Question text column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("qa_query_column_name") \
            .showErrorsFor("qa_query_column_name")
        qa_template = TextArea("Template", rows=8) \
            .bindProperty("qa_template")
        qa_column_selector = ColumnsLayout(gap="1rem") \
            .addElement(qa_context_column_selector) \
            .addElement(qa_query_column_selector)
        operation_container = StackLayout(gap="1rem") \
            .addElement(TitleElement("Operation")) \
            .addElement(operation_selector) \
            .addElement(iff("operation", "embed_texts", embed_column_selector)) \
            .addElement(iff("operation", "answer_question", qa_column_selector)) \
            .addElement(iff("operation", "answer_question", qa_template))

        # Model configuration container
        model_endpoint_selection = TextBox("Endpoint Name").bindProperty("model_endpoint_name")

        aws_region = SelectBox("AWS Region") \
            .addOption("us-east-1 - US East (N. Virginia)", "us-east-1") \
            .addOption("us-east-2 - US East (Ohio)", "us-east-2") \
            .addOption("us-west-1 - US West (N. California)", "us-west-1") \
            .addOption("us-west-2 - US West (Oregon)", "us-west-2") \
            .addOption("ap-south-1 - Asia Pacific (Mumbai)", "ap-south-1") \
            .addOption("ap-northeast-3 - Asia Pacific (Osaka)", "ap-northeast-3") \
            .addOption("ap-northeast-2 - Asia Pacific (Seoul)", "ap-northeast-2") \
            .addOption("ap-southeast-1 - Asia Pacific (Singapore)", "ap-southeast-1") \
            .addOption("ap-southeast-2 - Asia Pacific (Sydney)", "ap-southeast-2") \
            .addOption("ap-northeast-1 - Asia Pacific (Tokyo)", "ap-northeast-1") \
            .addOption("ca-central-1 - Canada (Central)", "ca-central-1") \
            .addOption("eu-central-1 - Europe (Frankfurt)", "eu-central-1") \
            .addOption("eu-west-1 - Europe (Ireland)", "eu-west-1") \
            .addOption("eu-west-2 - Europe (London)", "eu-west-2") \
            .addOption("eu-west-3 - Europe (Paris)", "eu-west-3") \
            .addOption("eu-north-1 - Europe (Stockholm)", "eu-north-1") \
            .addOption("sa-east-1 - South America (São Paulo)", "sa-east-1") \
            .bindProperty("aws_region")

        model_details = ColumnsLayout(gap="1rem") \
            .addElement(model_endpoint_selection) \
            .addElement(aws_region)

        model_props = ColumnsLayout(gap="1rem") \
            .addElement(TextBox("Max New Tokens").bindProperty("model_max_new_tokens")) \
            .addElement(TextBox("Top P").bindProperty("model_top_p")) \
            .addElement(TextBox("Model Temperature").bindProperty("model_temperature"))

        model_container = StackLayout(gap="1rem") \
            .addElement(TitleElement("Model configuration")) \
            .addElement(model_details) \
            .addElement(model_props)
            
        # Main container
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

    def validate(self, context: WorkflowContext, component: Component[SageMakerEndpointProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[SageMakerEndpointProperties], newState: Component[SageMakerEndpointProperties]) -> Component[
    SageMakerEndpointProperties]:
        return newState


    class SageMakerEndpointCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SageMakerEndpoint.SageMakerEndpointProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from spark_ai.llms.sagemaker import SageMakerLLM
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

            (SageMakerLLM(
                aws_access_key_id=access_key,
                aws_secret_access_key=access_secret,
                region_name=self.props.aws_region)
             .register_udfs(spark=self.spark))

            return in0 \
                .withColumn("_context", col(self.props.qa_context_column_name)) \
                .withColumn("_query", col(self.props.qa_query_column_name)) \
                .withColumn("_template", expr(""" " """ + self.props.qa_template + """ " """)) \
                .withColumn("_endpoint", lit(self.props.model_endpoint_name)) \
                .withColumn("_parameters", expr(f"""named_struct('max_new_tokens', {self.props.model_max_new_tokens}, 'top_p', {float(self.props.model_top_p)}, 'temperature', {float(self.props.model_temperature)})""")) \
                .withColumn("_attributes", lit("accept_eula=true")) \
                .withColumn("sagemaker_answer", expr("""sagemaker_answer_question(_context, _query, _template, _endpoint, _parameters, _attributes)""")) \
                .drop("_context", "_query", "_template", "_endpoint", "_parameters", "_attributes")            
