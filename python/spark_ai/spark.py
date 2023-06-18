import dataclasses
import typing
from typing import List, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import lit, struct, transform
from pyspark.sql.types import StructType, ArrayType, MapType, DataType, IntegerType, FloatType, BooleanType, StringType, \
    StructField


class SparkUtils:

    @staticmethod
    def _get_column(df: Union[DataFrame, Column], name: str, current_type: DataType, expected_type: DataType) -> Column:
        def with_alias(column: Column) -> Column:
            if len(name) > 0:
                return column.alias(name)
            else:
                return column

        df_element = df[name] if len(name) > 0 else df

        if isinstance(current_type, StructType) and isinstance(expected_type, StructType):
            nested_columns = SparkUtils._get_columns(df_element, current_type, expected_type)
            return with_alias(struct(*nested_columns))
        elif isinstance(current_type, ArrayType) and isinstance(expected_type, ArrayType):
            current_element_type = current_type.elementType
            expected_element_type = expected_type.elementType

            def array_element(row: Column):
                return SparkUtils._get_column(row, '', current_element_type, expected_element_type)

            return with_alias(transform(df_element, array_element))
        elif isinstance(current_type, MapType) and isinstance(expected_type, MapType):
            raise Exception("unsupported type map")
        else:
            return with_alias(df_element)

    @staticmethod
    def _get_columns(df: Union[DataFrame, Column], current_schema: StructType, expected_schema: StructType) -> List[
        Column]:
        columns = []
        for field in expected_schema.fields:
            if field.name in current_schema.names:
                current_column_type = current_schema[field.name].dataType
                columns.append(SparkUtils._get_column(df, field.name, current_column_type, field.dataType))
            else:
                columns.append(lit(None).cast(field.dataType).alias(field.name))

        return columns

    @staticmethod
    def default_missing_columns(df: DataFrame, expected_schema: StructType) -> DataFrame:
        columns = SparkUtils._get_columns(df, df.schema, expected_schema)
        return df.select(*columns)

    @staticmethod
    def dataclass_to_spark(tpe) -> StructType:
        if not dataclasses.is_dataclass(tpe):
            raise ValueError(f"Provided type is not a dataclass: {tpe}")

        return SparkUtils.type_to_spark(tpe)[0]

    @staticmethod
    def type_to_spark(tpe) -> (DataType, bool):
        type_mapping = {
            int: IntegerType(),
            float: FloatType(),
            bool: BooleanType(),
            str: StringType()
        }

        optional = False
        if typing.get_origin(tpe) is Union and type(None) in typing.get_args(tpe):
            tpe = list(filter(lambda t: not isinstance(t, type(None)), typing.get_args(tpe)))[0]
            optional = True

        if tpe in type_mapping:
            return type_mapping[tpe], optional
        elif typing.get_origin(tpe) is list:
            sub_type = typing.get_args(tpe)[0]
            (sub_spark_type, sub_optional) = SparkUtils.type_to_spark(sub_type)
            return ArrayType(sub_spark_type, containsNull=sub_optional), optional
        elif dataclasses.is_dataclass(tpe):
            fields = []
            for field in dataclasses.fields(tpe):
                (field_type, field_optional) = SparkUtils.type_to_spark(field.type)
                fields.append(StructField(field.name, field_type, nullable=field_optional))

            return StructType(fields), optional
        else:
            raise ValueError(f"Unsupported type: {tpe}")
