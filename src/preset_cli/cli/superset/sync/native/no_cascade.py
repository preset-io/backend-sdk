"""
No-cascade sync helpers extracted from the native sync command.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set, Tuple

import yaml

from preset_cli.exceptions import CLIError, SupersetError

AssetConfig = Dict[str, Any]
FindConfigByUUIDFn = Callable[
    [Dict[Path, AssetConfig], str, Any],
    Optional[Tuple[Path, AssetConfig]],
]
ResolveUUIDToIDFn = Callable[[Any, str, Any], Optional[int]]
ImportResourcesFn = Callable[[Dict[str, str], Any, bool, Any], None]


@dataclass(frozen=True)
class NoCascadePrimarySpec:
    """
    Configuration for resolving/creating primary no-cascade resources.
    """

    resource_name: str
    asset_type: Any
    build_contents: Callable[[Dict[Path, AssetConfig], Any, Any], Optional[Dict[str, str]]]
    missing_bundle_error: Callable[[str], str]


@dataclass(frozen=True)
class DashboardContentDeps:
    """
    Helpers required to build dashboard bundles.
    """

    find_config_by_uuid_fn: FindConfigByUUIDFn
    resolve_uuid_to_id_fn: ResolveUUIDToIDFn
    get_charts_uuids_fn: Callable[[AssetConfig], Any]
    get_dataset_filter_uuids_fn: Callable[[AssetConfig], Any]


@dataclass(frozen=True)
class NoCascadeContext:
    """
    Shared runtime context for no-cascade updates.
    """

    configs: Dict[Path, AssetConfig]
    client: Any
    overwrite: bool
    resolve_uuid_to_id_fn: ResolveUUIDToIDFn
    import_resources_fn: ImportResourcesFn
    logger: logging.Logger


@dataclass(frozen=True)
class ChartUpdateDeps:
    """
    Chart-specific dependencies required for no-cascade update flow.
    """

    chart_asset_type: Any
    dataset_asset_type: Any
    build_chart_contents_fn: Callable[[Dict[Path, AssetConfig], Any], Optional[Dict[str, str]]]
    build_dataset_contents_fn: Callable[[Dict[Path, AssetConfig], Any], Optional[Dict[str, str]]]
    prepare_chart_update_payload_fn: Callable[[AssetConfig, int, str, Any], Dict[str, Any]]


@dataclass(frozen=True)
class DashboardUpdateDeps:
    """
    Dashboard-specific dependencies required for no-cascade update flow.
    """

    dashboard_asset_type: Any
    build_dashboard_contents_fn: Callable[
        [Dict[Path, AssetConfig], Any, Any, bool],
        Optional[Dict[str, str]],
    ]
    prepare_dashboard_update_payload_fn: Callable[[AssetConfig, Any], Dict[str, Any]]


def prune_existing_dependency_configs(
    asset_configs: Dict[Path, AssetConfig],
    primary_path: Path,
    resource_name: str,
    dependency_resource_map: Dict[str, Dict[str, str]],
    resource_exists: Callable[[str, AssetConfig], bool],
) -> None:
    """
    Remove dependency configs that already exist when cascade is disabled.
    """
    dependency_map = dependency_resource_map.get(resource_name)
    if not dependency_map:
        return

    for dep_path, dep_config in list(asset_configs.items()):
        if dep_path == primary_path or len(dep_path.parts) < 2:
            continue

        dep_resource = dependency_map.get(dep_path.parts[1])
        if dep_resource and resource_exists(dep_resource, dep_config):
            asset_configs.pop(dep_path, None)


def find_config_by_uuid(
    configs: Dict[Path, AssetConfig],
    resource_dir: str,
    uuid_value: Any,
) -> Optional[Tuple[Path, AssetConfig]]:
    """
    Find a config by UUID in the given resource directory.
    """
    if not uuid_value:
        return None
    uuid_str = str(uuid_value)
    for path, config in configs.items():
        if len(path.parts) > 1 and path.parts[1] == resource_dir:
            if str(config.get("uuid")) == uuid_str:
                return path, config
    return None


def build_dataset_contents(
    configs: Dict[Path, AssetConfig],
    dataset_uuid: Any,
    find_config_by_uuid_fn: FindConfigByUUIDFn,
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a dataset (dataset + database).
    """
    dataset_entry = find_config_by_uuid_fn(configs, "datasets", dataset_uuid)
    if not dataset_entry:
        return None
    dataset_path, dataset_config = dataset_entry
    database_uuid = dataset_config.get("database_uuid")
    database_entry = find_config_by_uuid_fn(configs, "databases", database_uuid)
    if not database_entry:
        return None
    database_path, database_config = database_entry

    return {
        str(dataset_path): yaml.dump(dataset_config),
        str(database_path): yaml.dump(database_config),
    }


def build_chart_contents(
    configs: Dict[Path, AssetConfig],
    chart_uuid: Any,
    find_config_by_uuid_fn: FindConfigByUUIDFn,
    build_dataset_contents_fn: Callable[
        [Dict[Path, AssetConfig], Any, FindConfigByUUIDFn],
        Optional[Dict[str, str]],
    ],
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a chart (chart + dataset + database).
    """
    chart_entry = find_config_by_uuid_fn(configs, "charts", chart_uuid)
    if not chart_entry:
        return None
    chart_path, chart_config = chart_entry

    dataset_uuid = chart_config.get("dataset_uuid")
    dataset_contents = build_dataset_contents_fn(
        configs,
        dataset_uuid,
        find_config_by_uuid_fn,
    )
    if not dataset_contents:
        return None

    contents = {str(chart_path): yaml.dump(chart_config)}
    contents.update(dataset_contents)
    return contents


def _should_include_resource(
    missing_only: bool,
    client: Any,
    deps: DashboardContentDeps,
    resource_name: str,
    uuid_value: Any,
) -> bool:
    if not missing_only:
        return True
    if client is None:
        return True
    return deps.resolve_uuid_to_id_fn(client, resource_name, uuid_value) is None


def _add_dataset_contents(
    configs: Dict[Path, AssetConfig],
    dataset_uuid: Any,
    contents: Dict[str, str],
    deps: DashboardContentDeps,
    should_include_fn: Callable[[str, Any], bool],
) -> None:
    dataset_entry = deps.find_config_by_uuid_fn(configs, "datasets", dataset_uuid)
    if not dataset_entry:
        return
    dataset_path, dataset_config = dataset_entry
    if should_include_fn("dataset", dataset_uuid):
        contents[str(dataset_path)] = yaml.dump(dataset_config)

    database_uuid = dataset_config.get("database_uuid")
    database_entry = deps.find_config_by_uuid_fn(configs, "databases", database_uuid)
    if not database_entry:
        return
    database_path, database_config = database_entry
    if should_include_fn("database", database_uuid):
        contents[str(database_path)] = yaml.dump(database_config)


def build_dashboard_contents(
    configs: Dict[Path, AssetConfig],
    dashboard_uuid: Any,
    deps: DashboardContentDeps,
    client: Any = None,
    missing_only: bool = False,
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a dashboard and its dependencies.
    """
    dashboard_entry = deps.find_config_by_uuid_fn(configs, "dashboards", dashboard_uuid)
    if not dashboard_entry:
        return None
    dashboard_path, dashboard_config = dashboard_entry

    def should_include(resource_name: str, uuid_value: Any) -> bool:
        return _should_include_resource(
            missing_only,
            client,
            deps,
            resource_name,
            uuid_value,
        )

    contents: Dict[str, str] = {
        str(dashboard_path): yaml.dump(dashboard_config),
    }

    for chart_uuid in deps.get_charts_uuids_fn(dashboard_config):
        chart_entry = deps.find_config_by_uuid_fn(configs, "charts", chart_uuid)
        if not chart_entry:
            continue
        chart_path, chart_config = chart_entry
        if should_include("chart", chart_uuid):
            contents[str(chart_path)] = yaml.dump(chart_config)
        _add_dataset_contents(
            configs,
            chart_config.get("dataset_uuid"),
            contents,
            deps,
            should_include,
        )

    for dataset_uuid in deps.get_dataset_filter_uuids_fn(dashboard_config):
        _add_dataset_contents(
            configs,
            dataset_uuid,
            contents,
            deps,
            should_include,
        )

    return contents


def safe_json_loads(
    value: Any,
    label: str,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    Safely parse JSON content that may be a dict or string.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            logger.warning("Unable to parse %s as JSON", label)
            return None
    return None


def update_chart_datasource_refs(
    params: Optional[Dict[str, Any]],
    query_context: Optional[Dict[str, Any]],
    datasource_id: int,
    datasource_type: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Update datasource references in chart params/query_context.
    """
    dataset_uid = f"{datasource_id}__{datasource_type}"

    if params is not None:
        params["datasource"] = dataset_uid

    if query_context is not None:
        query_context["datasource"] = {"id": datasource_id, "type": datasource_type}
        form_data = query_context.get("form_data")
        if isinstance(form_data, dict):
            form_data["datasource"] = dataset_uid
        queries = query_context.get("queries")
        if isinstance(queries, list):
            for query in queries:
                if isinstance(query, dict) and "datasource" in query:
                    query["datasource"] = query_context["datasource"]

    return params, query_context


def filter_payload_to_schema(
    client: Any,
    resource_name: str,
    payload: Dict[str, Any],
    fallback_allowed: Set[str],
) -> Dict[str, Any]:
    """
    Filter payload keys based on API schema info when available.
    """
    try:
        info = client.get_resource_endpoint_info(resource_name, keys=["edit_columns"])
        allowed = {column["name"] for column in info.get("edit_columns", [])}
        if allowed:
            return {key: value for key, value in payload.items() if key in allowed}
    except SupersetError:
        pass

    return {key: value for key, value in payload.items() if key in fallback_allowed}


def prepare_chart_update_payload(  # pylint: disable=too-many-branches
    config: AssetConfig,
    datasource_id: int,
    datasource_type: str,
    client: Any,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Build a chart update payload from export config.
    """
    payload: Dict[str, Any] = {}

    for key in [
        "slice_name",
        "description",
        "viz_type",
        "cache_timeout",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    ]:
        if key in config:
            payload[key] = config[key]

    owners = config.get("owners")
    if owners:
        if all(isinstance(owner, int) for owner in owners):
            payload["owners"] = owners
        else:
            logger.warning("Skipping owners update for chart; owner IDs not available")

    tags = config.get("tags")
    if tags:
        if all(isinstance(tag, int) for tag in tags):
            payload["tags"] = tags
        else:
            logger.warning("Skipping tags update for chart; tag IDs not available")

    params_value = config.get("params")
    params = safe_json_loads(params_value, "chart params", logger)

    query_context_value = config.get("query_context")
    query_context = safe_json_loads(query_context_value, "chart query_context", logger)

    if params is not None or query_context is not None:
        params, query_context = update_chart_datasource_refs(
            params,
            query_context,
            datasource_id,
            datasource_type,
        )

    if params is not None:
        payload["params"] = json.dumps(params)
    elif isinstance(params_value, str):
        payload["params"] = params_value

    if query_context is not None:
        payload["query_context"] = json.dumps(query_context)
    elif isinstance(query_context_value, str):
        payload["query_context"] = query_context_value

    payload["datasource_id"] = datasource_id
    payload["datasource_type"] = datasource_type

    safe_allowlist = {
        "slice_name",
        "description",
        "viz_type",
        "owners",
        "params",
        "query_context",
        "cache_timeout",
        "datasource_id",
        "datasource_type",
        "dashboards",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
        "tags",
    }
    return filter_payload_to_schema(client, "chart", payload, safe_allowlist)


def set_integer_list_payload_field(
    payload: Dict[str, Any],
    config: AssetConfig,
    field_name: str,
    warning_message: str,
    logger: logging.Logger,
) -> None:
    """
    Add a list-valued payload field only when all values are integer IDs.
    """
    values = config.get(field_name)
    if not values:
        return
    if all(isinstance(value, int) for value in values):
        payload[field_name] = values
    else:
        logger.warning(warning_message)


def set_json_payload_field(
    payload: Dict[str, Any],
    config: AssetConfig,
    preferred_field: str,
    fallback_field: str,
    payload_field: str,
) -> None:
    """
    Add a JSON payload field with support for dict and serialized string values.
    """
    preferred_value = config.get(preferred_field)
    fallback_value = config.get(fallback_field)
    if isinstance(preferred_value, dict):
        payload[payload_field] = json.dumps(preferred_value)
    elif isinstance(fallback_value, dict):
        payload[payload_field] = json.dumps(fallback_value)
    elif isinstance(fallback_value, str):
        payload[payload_field] = fallback_value


def prepare_dashboard_update_payload(
    config: AssetConfig,
    client: Any,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Build a dashboard update payload from export config.
    """
    payload: Dict[str, Any] = {}

    for key in [
        "dashboard_title",
        "slug",
        "css",
        "published",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    ]:
        if key in config:
            payload[key] = config[key]

    set_integer_list_payload_field(
        payload,
        config,
        "owners",
        "Skipping owners update for dashboard; owner IDs not available",
        logger,
    )
    set_integer_list_payload_field(
        payload,
        config,
        "roles",
        "Skipping roles update for dashboard; role IDs not available",
        logger,
    )
    set_json_payload_field(
        payload,
        config,
        preferred_field="position",
        fallback_field="position_json",
        payload_field="position_json",
    )
    set_json_payload_field(
        payload,
        config,
        preferred_field="metadata",
        fallback_field="json_metadata",
        payload_field="json_metadata",
    )

    safe_allowlist = {
        "dashboard_title",
        "slug",
        "owners",
        "roles",
        "position_json",
        "css",
        "json_metadata",
        "published",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    }
    return filter_payload_to_schema(client, "dashboard", payload, safe_allowlist)


def resolve_or_create_primary_no_cascade(
    resource_uuid: str,
    context: NoCascadeContext,
    spec: NoCascadePrimarySpec,
) -> Optional[int]:
    """
    Resolve or create the primary resource for no-cascade update flows.
    """
    resource_label = spec.resource_name.capitalize()

    resource_id = context.resolve_uuid_to_id_fn(
        context.client,
        spec.resource_name,
        resource_uuid,
    )
    if resource_id is None:
        context.logger.info(
            "%s %s not found; attempting to create it",
            resource_label,
            resource_uuid,
        )
        contents = spec.build_contents(context.configs, resource_uuid, context.client)
        if not contents:
            raise CLIError(spec.missing_bundle_error(resource_uuid), 1)
        context.import_resources_fn(
            contents,
            context.client,
            overwrite=False,
            asset_type=spec.asset_type,
        )
        resource_id = context.resolve_uuid_to_id_fn(
            context.client,
            spec.resource_name,
            resource_uuid,
        )
        if resource_id is None:
            raise CLIError(f"Unable to create {spec.resource_name} {resource_uuid}", 1)
        if not context.overwrite:
            context.logger.info(
                "%s created; skipping update because overwrite=false",
                resource_label,
            )
            return None

    if not context.overwrite:
        context.logger.info(
            "Skipping existing %s %s (overwrite=false)",
            spec.resource_name,
            resource_uuid,
        )
        return None

    return resource_id


def update_chart_no_cascade(
    path: Path,
    config: AssetConfig,
    context: NoCascadeContext,
    deps: ChartUpdateDeps,
) -> None:
    """
    Update a chart via API without cascading to dependencies.
    """
    chart_uuid = config.get("uuid")
    if not chart_uuid:
        raise CLIError(f"Chart config missing UUID: {path}", 1)

    def build_chart_bundle(
        all_configs: Dict[Path, AssetConfig],
        uuid_value: Any,
        _client: Any,
    ) -> Optional[Dict[str, str]]:
        return deps.build_chart_contents_fn(all_configs, uuid_value)

    chart_id = resolve_or_create_primary_no_cascade(
        chart_uuid,
        context,
        NoCascadePrimarySpec(
            resource_name="chart",
            asset_type=deps.chart_asset_type,
            build_contents=build_chart_bundle,
            missing_bundle_error=lambda uuid: (
                f"Chart {uuid} not found and no dataset/database configs available."
            ),
        ),
    )
    if chart_id is None:
        return

    dataset_uuid = config.get("dataset_uuid")
    if not dataset_uuid:
        raise CLIError(f"Chart {chart_uuid} missing dataset_uuid", 1)

    dataset_id = context.resolve_uuid_to_id_fn(context.client, "dataset", dataset_uuid)
    if dataset_id is None:
        context.logger.info("Dataset %s not found; attempting to create it", dataset_uuid)
        contents = deps.build_dataset_contents_fn(context.configs, dataset_uuid)
        if not contents:
            raise CLIError(
                f"Dataset {dataset_uuid} not found and no dataset/database configs available.",
                1,
            )
        context.import_resources_fn(
            contents,
            context.client,
            overwrite=False,
            asset_type=deps.dataset_asset_type,
        )
        dataset_id = context.resolve_uuid_to_id_fn(context.client, "dataset", dataset_uuid)
        if dataset_id is None:
            raise CLIError(f"Unable to create dataset {dataset_uuid}", 1)

    dataset = context.client.get_dataset(dataset_id)
    datasource_type = dataset.get("kind") or dataset.get("datasource_type") or "table"

    payload = deps.prepare_chart_update_payload_fn(
        config,
        dataset_id,
        datasource_type,
        context.client,
    )
    context.logger.info("Updating chart %s via API (no-cascade)", chart_uuid)
    context.client.update_chart(chart_id, **payload)


def update_dashboard_no_cascade(
    path: Path,
    config: AssetConfig,
    context: NoCascadeContext,
    deps: DashboardUpdateDeps,
) -> None:
    """
    Update a dashboard via API without cascading to dependencies.
    """
    dashboard_uuid = config.get("uuid")
    if not dashboard_uuid:
        raise CLIError(f"Dashboard config missing UUID: {path}", 1)

    def build_dashboard_bundle(
        all_configs: Dict[Path, AssetConfig],
        uuid_value: Any,
        update_client: Any,
    ) -> Optional[Dict[str, str]]:
        return deps.build_dashboard_contents_fn(
            all_configs,
            uuid_value,
            update_client,
            True,
        )

    dashboard_id = resolve_or_create_primary_no_cascade(
        dashboard_uuid,
        context,
        NoCascadePrimarySpec(
            resource_name="dashboard",
            asset_type=deps.dashboard_asset_type,
            build_contents=build_dashboard_bundle,
            missing_bundle_error=lambda _uuid: (
                "Dashboard not found and no dashboard config available for import."
            ),
        ),
    )
    if dashboard_id is None:
        return

    payload = deps.prepare_dashboard_update_payload_fn(config, context.client)
    context.logger.info("Updating dashboard %s via API (no-cascade)", dashboard_uuid)
    context.client.update_dashboard(dashboard_id, **payload)
