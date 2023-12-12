from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *


class PineconeLookup(ComponentSpec):
    name: str = "PineconeLookup"
    category: str = "Machine Learning"
    gemDescription: str = "Lookup a vector embedding from a Pinecone Database"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/machine-learning/ml-pinecone-lookup"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class PineconeProperties(ComponentProperties):
        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "pinecone"
        credential_db_key: Optional[str] = "token"
        credential_manual_api_key: Optional[str] = ""

        environment: Optional[str] = "us-east-1-aws"
        index_name: Optional[str] = ""

        vector_column_name: Optional[str] = ""
        top_k: Optional[int] = 3

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

        credential_manual = TextBox("API Key") \
            .bindPlaceholder("80******-****-****-****-************") \
            .bindProperty("credential_manual_api_key")

        credential_db_or_manual = Condition() \
            .ifEqual(PropExpr("component.properties.credential_type"), StringExpr("databricks")) \
            .then(credential_db).otherwise(credential_manual)

        credential = StackLayout() \
            .addElement(credential_type) \
            .addElement(credential_db_or_manual)

        index_name_selector = TextBox("Index name") \
            .bindPlaceholder("") \
            .bindProperty("index_name")

        vector_column_name_selector = SchemaColumnsDropdown("Vector column (array<float>)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("vector_column_name") \
            .showErrorsFor("vector_column_name")

        k_selector = NumberBox("Number of results") \
            .bindProperty("top_k")

        properties = ColumnsLayout(gap="1rem") \
            .addElement(index_name_selector) \
            .addElement(vector_column_name_selector) \
            .addElement(k_selector)

        properties_container = StackLayout() \
            .addElement(TitleElement("Properties")) \
            .addElement(properties)

        # Main container
        main_container = StackLayout(padding="1rem", gap="3rem") \
            .addElement(credential) \
            .addElement(properties_container)

        return Dialog("OpenAI").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs().importSchema(), "2fr")
            .addColumn(main_container, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[PineconeProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[PineconeProperties], newState: Component[PineconeProperties]) -> Component[
        PineconeProperties]:
        return newState

    class PineconeLookupCode(ComponentCode):
        def __init__(self, props):
            self.props: PineconeLookup.PineconeProperties = props

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from pyspark.sql.functions import expr, array, struct
            from spark_ai.dbs.pinecone import PineconeDB, IdVector

            if self.props.credential_type == "databricks":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                api_key = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_key)
            else:
                api_key = self.props.credential_manual_api_key

            PineconeDB(api_key, self.props.environment).register_udfs(spark)

            return in0 \
                .withColumn("_vector", col(self.props.vector_column_name)) \
                .withColumn("_response",
                            expr(f"pinecone_query(\"{self.props.index_name}\", _vector, {self.props.top_k})")) \
                .withColumn("pinecone_matches", col("_response.matches")) \
                .withColumn("pinecone_error", col("_response.error")) \
                .drop("_vector", "_response")