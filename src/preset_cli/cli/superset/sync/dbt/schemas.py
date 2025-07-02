"""
Schemas and enums/TypeDicts used in the DBT sync process for Superset.
"""

from enum import Enum
from typing import Any, Dict, Type

from marshmallow import EXCLUDE, INCLUDE, Schema, fields, post_load, pre_load

# pylint: disable=invalid-name, too-few-public-methods


def parse_meta_properties(
    entity: Dict[str, Any],
    preserve_dbt_meta: bool = True,
) -> None:
    """
    Parses the meta properties from an entity.

    This helper performs two tasks:
    1. dbt is migrating from $entity.meta to $entity.config.meta, so it
    ensures `$entity.meta` gets the proper value.
    2. Pops `superset` properties from meta to a new `superset_meta` key.

    Setting `preserve_meta` to `False` would remove the `meta` key (useful when
    we don't care about dbt-specific meta properties).
    """
    entity["meta"] = entity.get("meta") or entity.get("config", {}).get("meta", {})
    entity["superset_meta"] = entity["meta"].pop("superset", {})
    if not preserve_dbt_meta:
        del entity["meta"]


class PostelSchema(Schema):
    """
    Be liberal in what you accept, and conservative in what you send.

    A schema that allows unknown fields. This way if the API returns new fields that
    the client is not expecting no errors will be thrown when validating the payload.
    """

    class Meta:
        """
        Ignore unknown and unnecessary fields.
        """

        unknown = INCLUDE


def PostelEnumField(enum: Type[Enum], *args: Any, **kwargs: Any) -> fields.Field:
    """
    Lenient replacement for ``EnumField``.

    This allows us to keep track of the enums expected in a field, while still
    accepting any unexpected new values that are introduced.
    """
    if issubclass(enum, str):
        return fields.String(*args, **kwargs)

    if issubclass(enum, int):
        return fields.Integer(*args, **kwargs)

    return fields.Raw(*args, **kwargs)


class AccountSchema(PostelSchema):
    """
    Schema for a dbt account.
    """

    id = fields.Integer()
    name = fields.String()


class ProjectSchema(PostelSchema):
    """
    Schema for a dbt project.
    """

    id = fields.Integer(allow_none=True)
    name = fields.String()


class JobSchema(PostelSchema):
    """
    Schema for a dbt job.
    """

    id = fields.Integer(allow_none=True)
    name = fields.String()


class ColumnSchema(PostelSchema):
    """
    Schema for a dbt model column.
    """

    class Meta:
        """
        Delete dbt-specific fields that won't be used by Superset.
        """

        unknown = EXCLUDE

    name = fields.String()
    verbose_name = fields.String(allow_none=True)
    description = fields.String(allow_none=True)
    superset_meta = fields.Raw(allow_none=True)

    @pre_load
    def parse_meta(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Parse the meta properties and populates the verbose name.
        """
        data["verbose_name"] = data["name"]
        parse_meta_properties(data, preserve_dbt_meta=False)
        return data

    @post_load
    def process_superset_metadata(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Automatically unpack Superset meta as top-level keys.
        """
        superset_meta = data.pop("superset_meta")
        return {
            **data,
            **superset_meta,
        }


class ModelSchema(PostelSchema):
    """
    Schema for a model.
    """

    depends_on = fields.List(fields.String())
    children = fields.List(fields.String())
    database = fields.String()
    schema = fields.String()
    description = fields.String()
    meta = fields.Raw()
    superset_meta = fields.Raw()
    name = fields.String()
    alias = fields.String(allow_none=True)
    unique_id = fields.String()
    tags = fields.List(fields.String())
    columns = fields.List(fields.Nested(ColumnSchema))
    config = fields.Dict(fields.String(), fields.Raw(allow_none=True))

    @pre_load
    def rename_fields(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Handle keys that can have camelCase or snake_case and Core/Cloud differences.
        """
        if "uniqueId" in data:
            data["unique_id"] = data.pop("uniqueId")

        if "childrenL1" in data:
            data["children"] = data.pop("childrenL1")

        if isinstance(columns := data.get("columns"), dict):
            data["columns"] = list(columns.values())

        if "dependsOn" in data:
            data["depends_on"] = data.pop("dependsOn")
        depends_on = data.get("depends_on", [])

        if isinstance(depends_on, dict):
            data["depends_on"] = depends_on["nodes"]

        parse_meta_properties(data)

        return data


class FilterSchema(PostelSchema):
    """
    Schema for a metric filter.
    """

    field = fields.String()
    operator = fields.String()
    value = fields.String()


class MetricSchema(PostelSchema):
    """
    Base schema for a dbt metric.
    """

    name = fields.String()
    label = fields.String()
    description = fields.String()
    meta = fields.Raw()
    superset_meta = fields.Raw()

    @pre_load
    def pre_load_process(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Parse the meta properties.
        """
        parse_meta_properties(data)
        return data


class OGMetricSchema(MetricSchema):
    """
    Schema for an OG metric.
    """

    depends_on = fields.List(fields.String())
    filters = fields.List(fields.Nested(FilterSchema))
    sql = fields.String()
    type = fields.String()
    unique_id = fields.String()
    # dbt >= 1.3
    calculation_method = fields.String()
    expression = fields.String()
    dialect = fields.String()
    skip_parsing = fields.Boolean(allow_none=True)

    @pre_load
    def pre_load_process(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Handle keys that can have camelCase or snake_case and Core/Cloud differences.
        """
        # Metric being loaded directly as OGMetric
        if "superset_meta" not in data:
            parse_meta_properties(data)

        if "uniqueId" in data:
            data["unique_id"] = data.pop("uniqueId")

        if "dependsOn" in data:
            data["depends_on"] = data.pop("dependsOn")
        depends_on = data.get("depends_on", [])

        if isinstance(depends_on, dict):
            data["depends_on"] = depends_on["nodes"]

        return data


class MFMetricType(str, Enum):
    """
    Type of the MetricFlow metric.
    """

    SIMPLE = "SIMPLE"
    RATIO = "RATIO"
    CUMULATIVE = "CUMULATIVE"
    DERIVED = "DERIVED"


class MFMetricSchema(MetricSchema):
    """
    Schema for a MetricFlow metric.
    """

    type = PostelEnumField(MFMetricType)


class MFSQLEngine(str, Enum):
    """
    Databases supported by MetricFlow.
    """

    BIGQUERY = "BIGQUERY"
    DUCKDB = "DUCKDB"
    REDSHIFT = "REDSHIFT"
    POSTGRES = "POSTGRES"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"


class MFMetricWithSQLSchema(MFMetricSchema):
    """
    MetricFlow metric with dialect and SQL, as well as model.
    """

    sql = fields.String()
    dialect = PostelEnumField(MFSQLEngine)
    model = fields.String()
