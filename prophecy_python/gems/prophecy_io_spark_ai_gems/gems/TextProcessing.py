from pyspark.sql import *
from pyspark.sql.functions import *
from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
class TextProcessing(ComponentSpec):
    name: str = "TextProcessing"
    category: str = "Machine Learning"
    gemDescription: str = "Text processing to prepare data to submit to a foundational model API."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/machine-learning/ml-text-processing"
    def optimizeCode(self) -> bool:
        return True
    @dataclass(frozen=True)
    class TextProcessingProperties(ComponentProperties):
        operation: Optional[str] = ""
        column_name: Optional[str] = ""
        split_chunk_size: int = 1000
    def dialog(self) -> Dialog:
        def iff(property_name: str, value, then: Atom) -> Condition:
            value_expr = BooleanExpr(value) if isinstance(value, bool) else StringExpr(str(value))
            return Condition().ifEqual(PropExpr(f"component.properties.{property_name}"), value_expr).then(then) 
        # Operation selection
        operation_selector = SelectBox("Operation type") \
            .addOption("Split text into equal chunks", "text_split_into_chunks") \
            .addOption("Load url (web scrape)", "web_scrape") \
            .addOption("Load url (web scrape) and extract text", "web_scrape_text") \
            .addOption("Parse pdf and extract text", "pdf_parse") \
            .addOption("Parse docx and extract text", "docx_parse") \
            .bindProperty("operation")
        operation_container = StackLayout(gap="1rem") \
            .addElement(TitleElement("Operation")) \
            .addElement(operation_selector)
        # Operation: Text split into chunks
        chunks_column_name_selector = SchemaColumnsDropdown("Column name (string)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("column_name") \
            .showErrorsFor("column_name")
        chunks_field = NumberBox("Size of the chunk to split the text into") \
            .bindProperty("split_chunk_size")
        chunks_container = ColumnsLayout(gap="1rem") \
            .addElement(chunks_column_name_selector) \
            .addElement(chunks_field)
        # Operation: Load url (web scrape)
        web_scrape_container = SchemaColumnsDropdown("Column name (string with urls)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("column_name") \
            .showErrorsFor("column_name")
        # Operation: Parse pdf
        pdf_parse_container = SchemaColumnsDropdown("Column name (binary)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("column_name") \
            .showErrorsFor("column_name")
        # Operation: Parse docx
        docx_parse_container = SchemaColumnsDropdown("Column name (binary)") \
            .bindSchema("component.ports.inputs[0].schema") \
            .bindProperty("column_name") \
            .showErrorsFor("column_name")
        # Main container
        main_container = StackLayout(padding="1rem", gap="3rem") \
            .addElement(operation_selector) \
            .addElement(iff("operation", "text_split_into_chunks", chunks_container)) \
            .addElement(iff("operation", "web_scrape", web_scrape_container)) \
            .addElement(iff("operation", "web_scrape_text", web_scrape_container)) \
            .addElement(iff("operation", "pdf_parse", pdf_parse_container)) \
            .addElement(iff("operation", "docx_parse", docx_parse_container))

        return Dialog("OpenAI").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs().importSchema(), "2fr")
            .addColumn(main_container, "5fr")
        )
    def validate(self, context: WorkflowContext, component: Component[TextProcessingProperties]) -> List[Diagnostic]:
        return []
    def onChange(self, context: WorkflowContext, oldState: Component[TextProcessingProperties], newState: Component[TextProcessingProperties]) -> \
            Component[
                TextProcessingProperties]:
        return newState
    class TextProcessingCode(ComponentCode):
        def __init__(self, props):
            self.props: TextProcessing.TextProcessingProperties = props
        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from pyspark.sql.functions import expr, array, struct
            if self.props.operation == "text_split_into_chunks":
                from spark_ai.files.text import FileTextUtils
                FileTextUtils().register_udfs(spark)
                column_name = self.props.column_name
                chunk_size = str(self.props.split_chunk_size)
                return in0.withColumn("result_chunks", expr(f"text_split_into_chunks({column_name}, {chunk_size})"))
            elif self.props.operation == "web_scrape":
                from spark_ai.webapps import WebUtils
                WebUtils().register_udfs(spark)
                return in0.withColumn("result_content", expr(f"web_scrape({self.props.column_name})"))
            elif self.props.operation == "web_scrape_text":
                from spark_ai.webapps import WebUtils
                WebUtils().register_udfs(spark)
                return in0.withColumn("result_content", expr(f"web_scrape_text({self.props.column_name})"))
            elif self.props.operation == "pdf_parse":
                from spark_ai.files.pdf import FilePDFUtils
                FilePDFUtils().register_udfs(spark)
                return in0.withColumn("result_content", expr(f"pdf_parse({self.props.column_name})"))
            elif self.props.operation == "docx_parse":
                from spark_ai.files.docx import FileDocxUtils
                FileDocxUtils().register_udfs(spark)
                return in0.withColumn("result_content", expr(f"docx_parse({self.props.column_name})"))