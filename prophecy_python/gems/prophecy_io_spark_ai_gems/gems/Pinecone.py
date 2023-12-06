from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import row_number, ceil, collect_list
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, FloatType
from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *


class PineconeFormatSpec(DatasetSpec):
    name: str = "pinecone"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/text"

    def optimizeCode(self) -> bool:
        return False

    @dataclass(frozen=True)
    class PineconeProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""

        credential_type: Optional[str] = "databricks"
        credential_db_scope: Optional[str] = "pinecone"
        credential_db_key: Optional[str] = "token"
        credential_manual_api_key: Optional[str] = ""

        environment: Optional[str] = "us-east-1-aws"
        index_name: Optional[str] = ""

        vector_id_column_name: Optional[str] = ""
        vector_content_column_name: Optional[str] = ""

        status: bool = True
        status_is_uc: Optional[bool] = None
        status_catalog: Optional[str] = None
        status_database: str = ""
        status_table: str = ""

    def sourceDialog(self) -> DatasetDialog: 
        return DatasetDialog("pinecone")

    def targetDialog(self) -> DatasetDialog:
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

        # Pinecone env & index
        env_selector = TextBox("Environment") \
            .bindPlaceholder("us-east-1-aws") \
            .bindProperty("environment")

        index_selector = TextBox("Index name") \
            .bindProperty("index_name")

        location_selector = ColumnsLayout(gap="1rem") \
            .addElement(env_selector) \
            .addElement(index_selector)

        location_section = StackLayout() \
            .addElement(TitleElement("Location")) \
            .addElement(location_selector)

        # Status writing
        status_selector = CatalogTableDB("") \
            .bindProperty("status_database") \
            .bindTableProperty("status_table") \
            .bindCatalogProperty("status_catalog") \
            .bindIsCatalogEnabledProperty("status_is_uc")

        status_description = "When this option is enabled, the Target component is going to write the " \
                             "status of Pinecone writes, to a Delta table for further querying."
        status = StackLayout() \
            .addElement(TitleElement("Status writing")) \
            .addElement(NativeText(status_description)) \
            .addElement(Checkbox("Enable status write (recommended)").bindProperty("status")) \
            .addElement(iff("status", True, status_selector))

        location = StackLayout() \
            .addElement(credential) \
            .addElement(location_section) \
            .addElement(status)

        location.padding = "1rem"
        location.gap = "3rem"

        # -------------------
        # PROPERTIES SECTION
        # -------------------

        id_column_selector = SchemaColumnsDropdown("Vector id column (expected type: str)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("vector_id_column_name") \
            .showErrorsFor("vector_id_column_name")

        content_column_selector = SchemaColumnsDropdown("Vector content column (expected type: array<float>)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("vector_content_column_name") \
            .showErrorsFor("vector_content_column_name")

        # columns_selector = ColumnsLayout(gap="1rem", height="80px") \
        #     .addElement(id_column_selector) \
        #     .addElement(content_column_selector)

        return DatasetDialog("pinecone") \
            .addSection("LOCATION", location) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
            .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                    .addElement(
                        StackItem(grow=(1))
                        .addElement(
                            FieldPicker(height=("100%"))
                            .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                            .addField(id_column_selector, "vector_id_column_name", True)
                            .addField(content_column_selector, "vector_content_column_name", True)
                        )
                    )
                ),
                "auto"
            )
            .addColumn(SchemaTable("").withoutInferSchema().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        return super(PineconeFormatSpec, self).validate(context, component)

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState
        # return newState.bindProperties(replace(newState.properties, schema=StructType([
        #     StructField("id_vectors", ArrayType(StructType([
        #         StructField("id", StringType()),
        #         StructField("vector", ArrayType(FloatType()))
        #     ]))),
        #     StructField("count", IntegerType()),
        #     StructField("error", StringType())
        # ])))

    class PineconeFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: PineconeFormatSpec.PineconeProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            return spark.range(0)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            from pyspark.sql.functions import expr, array, struct
            from spark_ai.dbs.pinecone import PineconeDB, IdVector

            if self.props.credential_type == "databricks":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                api_key = dbutils.secrets.get(scope=self.props.credential_db_scope, key=self.props.credential_db_key)
            else:
                api_key = self.props.credential_manual_api_key

            PineconeDB(api_key, self.props.environment).register_udfs(spark)

            df_upserted = in0 \
                .withColumn("_row_num",
                            row_number().over(Window.partitionBy().orderBy(col(self.props.vector_id_column_name)))) \
                .withColumn("_group_num", ceil(col("_row_num") / 20)) \
                .withColumn("_id_vector", struct(col(self.props.vector_id_column_name).alias("id"),
                                                 col(self.props.vector_content_column_name).alias("vector"))) \
                .groupBy(col("_group_num")) \
                .agg(collect_list(col("_id_vector")).alias("id_vectors")) \
                .withColumn("upserted", expr(f"pinecone_upsert(\"{self.props.index_name}\", id_vectors)")) \
                .select(col("*"), col("upserted.*")) \
                .select(col("id_vectors"), col("count"), col("error"))

            if self.props.status:
                status_table = f"{self.props.status_catalog}.{self.props.status_database}.{self.props.status_table}" if self.props.status_is_uc else f"{self.props.status_database}.{self.props.status_table}"
                if spark.catalog.tableExists(status_table):
                    df_upserted.write.format("delta").insertInto(status_table)
                else:
                    df_upserted.write.format("delta").mode("overwrite").saveAsTable(status_table)
            else:
                df_upserted.count()