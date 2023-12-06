from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.datatypes import SInt
from prophecy.cb.ui.uispec import *


class OpenAISpec(ComponentSpec): 
    name: str = "OpenAI"
    category: str = "Machine Learning"
    gemDescription: str = "Request OpenAI to generate a vector embedding or request OpenAI to answer a question with an optional context."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/machine-learning/ml-openai"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class OpenAIProperties(ComponentProperties):
        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "open_ai"
        credential_db_key: Optional[str] = "token"
        credential_manual_token: Optional[str] = ""

        # Potential values: embed_texts, chat_complete, answer_question
        operation: str = "embed_texts"

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
            .addElement(TextBox("Databricks Key").bindProperty("credential_db_key"))

        credential_manual = TextBox("Token") \
            .bindPlaceholder("xoxb-***********-*************-************************") \
            .bindProperty("credential_manual_token")

        credential_db_or_manual = Condition() \
            .ifEqual(PropExpr("component.properties.credential_type"), StringExpr("databricks")) \
            .then(credential_db).otherwise(credential_manual)

        credential = StackLayout() \
            .addElement(credential_type) \
            .addElement(credential_db_or_manual)

        operation_selector = SelectBox("Operation type") \
            .addOption("Compute text embeddings", "embed_texts") \
            .addOption("Answer questions", "chat_complete") \
            .addOption("Answer questions for given context", "answer_question") \
            .bindProperty("operation")

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

        # Main container
        main_container = StackLayout(padding="1rem", gap="2rem") \
            .addElement(credential) \
            .addElement(operation_container)

        main_container.padding = "1rem"
        main_container.gap = "3rem"

        return Dialog("OpenAI").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs().importSchema(), "2fr")
            .addColumn(main_container, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[OpenAIProperties]) -> List[Diagnostic]:
        return []

    def onChange(
            self,
            context: WorkflowContext,
            oldState: Component[OpenAIProperties],
            newState: Component[OpenAIProperties]
    ) -> Component[OpenAIProperties]:
        return newState

    class OpenAICode(ComponentCode):
        def __init__(self, newProps):
            self.props: OpenAISpec.OpenAIProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from spark_ai.llms.openai import OpenAiLLM

            api_key = ""
            if self.props.credential_type == "databricks":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                api_key = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_key)
            else:
                api_key = self.props.credential_manual_token

            OpenAiLLM(api_key=api_key).register_udfs(spark=spark)

            if self.props.operation == "embed_texts":
                if not self.props.group_data:
                    return in0 \
                        .withColumn("_texts", array(col(self.props.embed_text_column_name))) \
                        .withColumn("_embedded", expr("openai_embed_texts(_texts)")) \
                        .withColumn("openai_embedding", expr("_embedded.embeddings[0]")) \
                        .withColumn("openai_error", col("_embedded.error")) \
                        .drop("_texts", "_embedded")
                else:
                    return in0 \
                        .withColumn("_row_num",
                                    row_number().over(Window.partitionBy().orderBy(col(self.props.order_by_column)))) \
                        .withColumn("_group_num", ceil(col("_row_num") / 20)) \
                        .withColumn("_data", struct(col("*"))) \
                        .groupBy(col("_group_num")) \
                        .agg(collect_list(col("_data")).alias("_data"),
                             collect_list(col(self.props.embed_text_column_name)).alias("_texts")) \
                        .withColumn("_embedded", expr(f"openai_embed_texts(_texts)")) \
                        .select(col("_texts").alias("_texts"), col("_embedded.embeddings").alias("_embeddings"),
                                col("_embedded.error").alias("openai_error"), col("_data")) \
                        .select(expr("explode_outer(arrays_zip(_embeddings, _data))").alias("_content"),
                                col("openai_error")) \
                        .select(col("_content._embeddings").alias("openai_embedding"), col("openai_error"),
                                col("_content._data.*")) \
                        .drop("_row_num").drop("_group_num")

            elif self.props.operation == "answer_question":
                return in0 \
                    .withColumn("_context", col(self.props.qa_context_column_name)) \
                    .withColumn("_query", col(self.props.qa_query_column_name)) \
                    .withColumn("openai_answer",
                                expr(
                                    """openai_answer_question(_context, _query, " """ + self.props.qa_template + """ ")""")) \
                    .drop("_context", "_query")
            else:
                return in0