from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
class BinaryFormat(DatasetSpec):
    name: str = "binary"
    datasetType: str = "File" 
    def optimizeCode(self) -> bool:
        return True
    @dataclass(frozen=True)
    class BinaryProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        useSchema: Optional[bool] = True
        path: str = ""
        writeMode: Optional[str] = None
        compression: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        lineSep: Optional[str] = None
        recursiveFileLookup: Optional[bool] = None
    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("binary") \
            .addSection("LOCATION", TargetLocation("path")) \
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
                                .addField(Checkbox("Enforce schema"), "useSchema", True)
                                .addField(Checkbox("Read file as single row"), "wholetext")
                                .addField(TextBox("Line Separator", allowEscapeSequence=True).bindPlaceholder(""),
                                          "lineSep")
                                .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )
    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("binary") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                                .addField(
                                SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                "writeMode"
                            )
                                .addField(
                                SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .showErrorsFor("partitionColumns"),
                                "partitionColumns"
                            )
                                .addField(
                                SelectBox("Compression Codec")
                                    .addOption("none", "none")
                                    .addOption("bzip2", "bzip2")
                                    .addOption("gzip", "gzip")
                                    .addOption("lz4", "lz4")
                                    .addOption("snappy", "snappy")
                                    .addOption("deflate", "deflate"),
                                "compression"
                            )
                                .addField(TextBox("Line Separator", allowEscapeSequence=True).bindPlaceholder(""),
                                          "lineSep")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )
    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(BinaryFormat, self).validate(context, component)
        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))
        from prophecy.cb.util.StringUtils import isBlank
        if component.properties.lineSep is not None and isBlank(component.properties.lineSep):
            diagnostics.append(
                Diagnostic("properties.lineSep", "Line Separator cannot be empty [Properties]",
                           SeverityLevelEnum.Error))
        return diagnostics
    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState
    class BinaryFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: BinaryFormat.BinaryProperties = props
        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("binaryFile")
            if self.props.recursiveFileLookup is not None:
                reader = reader.option("recursiveFileLookup", self.props.recursiveFileLookup)
            if self.props.schema is not None and self.props.useSchema:
                reader = reader.schema(self.props.schema)
            return reader.load(self.props.path)
        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("binaryFile")
            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)
            writer.save(self.props.path)