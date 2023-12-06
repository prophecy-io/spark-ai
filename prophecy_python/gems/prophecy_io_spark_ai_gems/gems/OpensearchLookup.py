from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class OpenSearchLookup(ComponentSpec):
    name: str = "OpenSearchLookup"
    category: str = "Machine Learning"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class OpenSearchLookupProperties(ComponentProperties):
        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "opensearch"

        credential_db_key: Optional[str] = "token"
        credential_db_secrets: Optional[str] = "secrets"

        credential_manual_api_key: Optional[str] = ""
        credential_manual_api_secrets: Optional[str] = "" 

        host: Optional[str] = ""
        region: Optional[str] = "us-west-1"
        service: Optional[str] = "aoss"
        index_name: Optional[str] = ""
        
        vector_id_column_name: Optional[str] = ""
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
            .addElement(TextBox("Databricks Key").bindProperty("credential_db_key")) \
            .addElement(TextBox("Databricks Secrets").bindProperty("credential_db_secrets"))


        credential_manual = ColumnsLayout(gap="1rem") \
            .addElement(TextBox("Aws Key").bindPlaceholder("").bindProperty("credential_manual_api_key")) \
            .addElement(TextBox("Aws Secrets").bindPlaceholder("").bindProperty("credential_manual_api_secrets"))
        
        credential_db_or_manual = Condition() \
            .ifEqual(PropExpr("component.properties.credential_type"), StringExpr("databricks")) \
            .then(credential_db).otherwise(credential_manual)

        credential = StackLayout() \
            .addElement(credential_type) \
            .addElement(credential_db_or_manual)

        host_selector = TextBox("Host") \
            .bindPlaceholder("") \
            .bindProperty("host")
        
        region_selector = TextBox("Aws Region") \
            .bindPlaceholder("") \
            .bindProperty("region")

        service_selector = TextBox("Aws Service") \
            .bindPlaceholder("") \
            .bindProperty("service")
        
        location = ColumnsLayout(gap="1rem") \
            .addElement(host_selector) \
            .addElement(region_selector) \
            .addElement(service_selector) 
            

        location_container = StackLayout() \
            .addElement(TitleElement("Location")) \
            .addElement(location)

        index_name_selector = TextBox("Index Name") \
            .bindPlaceholder("") \
            .bindProperty("index_name")

        vector_id_column_name_selector = TextBox("Vector Id Column") \
            .bindPlaceholder("vector id column") \
            .bindProperty("vector_id_column_name")

        vector_column_name_selector = SchemaColumnsDropdown("Vector Embeddiing Column") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("vector_column_name") \
            .showErrorsFor("vector_column_name")
        
        k_selector = NumberBox("Number of results") \
            .bindProperty("top_k")

        properties = ColumnsLayout(gap="1rem") \
            .addElement(index_name_selector) \
            .addElement(vector_id_column_name_selector) \
            .addElement(vector_column_name_selector) \
            .addElement(k_selector)

        properties_container = StackLayout() \
            .addElement(TitleElement("Properties")) \
            .addElement(properties)

        # Main container
        main_container = StackLayout(padding="1rem", gap="3rem") \
            .addElement(credential) \
            .addElement(location_container) \
            .addElement(properties_container)

        return Dialog("OpenAI").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs().importSchema(), "2fr")
            .addColumn(main_container, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[OpenSearchLookupProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[OpenSearchLookupProperties], newState: Component[OpenSearchLookupProperties]) -> Component[
    OpenSearchLookupProperties]:
        return newState


    class OpenSearchLookupCode(ComponentCode):
        def __init__(self, newProps):
            self.props: OpenSearchLookup.OpenSearchLookupProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from pyspark.sql.functions import expr, array, struct
            from spark_ai.dbs.opensearch import OpensearchDB

            if self.props.credential_type == "databricks":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                usr = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_key)
                secrets = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_secrets)
            else:
                usr = self.props.credential_manual_api_key
                secrets = self.props.credential_manual_api_secrets
            
            host = self.props.host
            region = self.props.region
            service = self.props.service

            OpensearchDB(host,region,usr,secrets,service).register_udfs(spark)

            return in0 \
                .withColumn("_vector", col(self.props.vector_column_name)) \
                .withColumn("_response",expr(f"opensearch_query(\"{self.props.index_name}\",\"{self.props.vector_id_column_name}\", \"{self.props.vector_column_name}\", _vector, {self.props.top_k})")) \
                .withColumn("opensearch_matches", col("_response.matches")) \
                .withColumn("opensearch_error", col("_response.error")) \
                .drop("_vector", "_response")
