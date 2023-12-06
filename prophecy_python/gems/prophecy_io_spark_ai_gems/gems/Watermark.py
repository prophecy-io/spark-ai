from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *

from prophecy.cb.ui.UISpecUtil import sanitizedColumn
from prophecy.cb.ui.uispec import *


from prophecy.cb.server.base import WorkflowContext

from pyspark.sql import *
from prophecy.cb.ui.uispec import *



@dataclass(frozen=True)
class WatermarkRule:
    portId: str
    portSlug: str
    isStreamingDF: bool
    disableTextBox: bool
    expression: str
    watermarkValue: str


class Watermark(ComponentSpec):
    name: str = "Watermark"
    category: str = "Transform"
    gemDescription: str = "We have added a Watermarking Gem in the Transform Section that allows a user to add a Watermark to a DataFrame."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/spark-streaming/transformations-streaming"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class WatermarkProperties(ComponentProperties):
        watermarks: Optional[List[WatermarkRule]] = field(default_factory=lambda: [WatermarkRule("in0", "in0", False, True, "", "")])
        isWatermarkingEnabled: Optional[bool] = True


    def onClickPSTab(self, portId: str, column: str, state: Component[WatermarkProperties]):
        outputWatermarks = []
        for wtr in state.properties.watermarks:
            if wtr.portId != portId:
                outputWatermarks.append(wtr)
            else:
                portSlug = [prt.slug for prt in state.ports.inputs if prt.id == portId][0]
                isStreamingDF = [prt.isStreaming for prt in state.ports.inputs if prt.id == portId][0]
                newWatermark = WatermarkRule(portId, portSlug, isStreamingDF, not isStreamingDF, sanitizedColumn(column), wtr.watermarkValue)
                outputWatermarks.append(newWatermark)
        isWatermarkingEnabled = True # any([wtr.isStreamingDF for wtr in outputWatermarks])
        return state.bindProperties(
            replace(state.properties, watermarks=outputWatermarks, isWatermarkingEnabled=isWatermarkingEnabled)
        )
    def dialog(self) -> Dialog:
        watermarkTable = BasicTable("Watermark Table", columns=[
            Column("Input Alias", "portSlug", TextBox("", placeholder="in0", disabledView=True)),
            Column(
                "Watermark on",
                "record.expression",
                TextBox("", placeholder="timestamp Column To Watermark On", disabledView="${record.disableTextBox}").bindProperty("record.expression"),
            ),
            Column(
                "Watermark Duration",
                "watermarkValue",
                TextBox("", placeholder="5 minutes", disabledView="${record.disableTextBox}").bindProperty("record.watermarkValue"),
                width="25%")
        ], appendNewRow=False).bindProperty("watermarks")
        return Dialog("Watermark").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(PortSchemaTabs(singleColumnClickCallback=self.onClickPSTab).importSchema(), "2fr")
            .addColumn(
                Condition().ifEqual(
                    PropExpr(f"component.properties.isWatermarkingEnabled"), BooleanExpr(True)
                ).then(watermarkTable).otherwise(
                    AlertBox("warning").addElement(NativeText("Watermark Options not available for Batch Dataframe"))
                ), "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[WatermarkProperties]) -> List[Diagnostic]:
        from prophecy.cb.server.base.ComponentBuilderBase import Diagnostic, SeverityLevelEnum
        diagnostics = []
        enableDiagnostic = False
        if component.properties.isWatermarkingEnabled:
            for watermark in component.properties.watermarks:
                if watermark.isStreamingDF and (watermark.expression=="" or watermark.watermarkValue==""):
                    enableDiagnostic = True
        if enableDiagnostic:
            diagnostics.append(
                Diagnostic(
                    "properties.watermark",
                    "Not defining Watermarks can lead to OOM Errors while aggregating Streaming Datasets",
                    SeverityLevelEnum.Warning,
                )
            )
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[WatermarkProperties], newState: Component[WatermarkProperties]) -> Component[
        WatermarkProperties]:
        newProps = newState.properties
        newInputPorts = newState.ports.inputs
        watermarksRuleList = newProps.watermarks
        outputWatermarks = []

        for newPort in newInputPorts:
            toggle = False
            portId = newPort.id
            for watermark in watermarksRuleList:
                if watermark.portId == portId:
                    outputWatermarks.append(WatermarkRule(portId, newPort.slug, newPort.isStreaming, not newPort.isStreaming, watermark.expression, watermark.watermarkValue))
                    toggle = True
            if not toggle:
                outputWatermarks.append(WatermarkRule(portId, newPort.slug, newPort.isStreaming, not newPort.isStreaming, "", ""))
        isWatermarkingEnabled = True # any([wtr.isStreamingDF for wtr in outputWatermarks])
        return newState.bindProperties(replace(newProps, watermarks=outputWatermarks, isWatermarkingEnabled=isWatermarkingEnabled))


    class WatermarkCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Watermark.WatermarkProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            if self.props.watermarks is not None and self.props.watermarks[0].watermarkValue !="":
                in0_withWatermark = in0.withWatermark(self.props.watermarks[0].expression, self.props.watermarks[0].watermarkValue)
            else:
                in0_withWatermark = in0
            return in0_withWatermark