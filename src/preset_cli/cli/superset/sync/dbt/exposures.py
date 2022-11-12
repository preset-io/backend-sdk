"""
Sync Superset dashboards as dbt exposures.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional

import yaml

from preset_cli.api.clients.superset import SupersetClient

# XXX: DashboardResponseType and DatasetResponseType


class ModelKey(NamedTuple):
    """
    Model key, so they can be mapped from datasets.
    """

    schema: Optional[str]
    table: str


def get_chart_depends_on(
    client: SupersetClient,
    chart: Any,
    model_map: Dict[ModelKey, str],
) -> List[str]:
    """
    Get all the dbt dependencies for a given chart.
    """

    query_context = json.loads(chart["query_context"])
    dataset_id = query_context["datasource"]["id"]
    dataset = client.get_dataset(dataset_id)
    extra = json.loads(dataset["extra"] or "{}")
    if "depends_on" in extra:
        return [extra["depends_on"]]

    key = ModelKey(dataset["schema"], dataset["table_name"])
    if dataset["datasource_type"] == "table" and key in model_map:
        return [model_map[key]]

    return []


def get_dashboard_depends_on(
    client: SupersetClient,
    dashboard: Any,
    model_map: Dict[ModelKey, str],
) -> List[str]:
    """
    Get all the dbt dependencies for a given dashboard.
    """

    url = client.baseurl / "api/v1/dashboard" / str(dashboard["id"]) / "datasets"

    session = client.auth.session
    headers = client.auth.get_headers()
    response = session.get(url, headers=headers)
    response.raise_for_status()

    payload = response.json()

    depends_on = []
    for dataset in payload["result"]:
        full_dataset = client.get_dataset(int(dataset["id"]))
        try:
            extra = json.loads(full_dataset["extra"] or "{}")
        except json.decoder.JSONDecodeError:
            extra = {}

        key = ModelKey(full_dataset["schema"], full_dataset["table_name"])
        if "depends_on" in extra:
            depends_on.append(extra["depends_on"])
        elif full_dataset["datasource_type"] == "table" and key in model_map:
            depends_on.append(model_map[key])

    return depends_on


def sync_exposures(  # pylint: disable=too-many-locals
    client: SupersetClient,
    exposures_path: Path,
    datasets: List[Any],
    model_map: Dict[ModelKey, str],
) -> None:
    """
    Write dashboards back to dbt as exposures.
    """
    exposures = []
    charts_ids = set()
    dashboards_ids = set()

    for dataset in datasets:
        url = client.baseurl / "api/v1/dataset" / str(dataset["id"]) / "related_objects"

        session = client.auth.session
        headers = client.auth.get_headers()
        response = session.get(url, headers=headers)
        response.raise_for_status()

        payload = response.json()
        for chart in payload["charts"]["result"]:
            charts_ids.add(chart["id"])
        for dashboard in payload["dashboards"]["result"]:
            dashboards_ids.add(dashboard["id"])

    for chart_id in charts_ids:
        chart = client.get_chart(chart_id)
        first_owner = chart["owners"][0]
        exposure = {
            "name": chart["slice_name"] + " [chart]",
            "type": "analysis",
            "maturity": "high" if chart["certified_by"] else "low",
            "url": str(
                client.baseurl
                / "superset/explore/"
                % {"form_data": json.dumps({"slice_id": chart_id})},
            ),
            "description": chart["description"] or "",
            "depends_on": get_chart_depends_on(client, chart, model_map),
            "owner": {
                "name": first_owner["first_name"] + " " + first_owner["last_name"],
                "email": first_owner.get("email", "unknown"),
            },
        }
        exposures.append(exposure)

    for dashboard_id in dashboards_ids:
        dashboard = client.get_dashboard(dashboard_id)
        first_owner = dashboard["owners"][0]
        exposure = {
            "name": dashboard["dashboard_title"] + " [dashboard]",
            "type": "dashboard",
            "maturity": "high"
            if dashboard["published"] or dashboard["certified_by"]
            else "low",
            "url": str(client.baseurl / dashboard["url"].lstrip("/")),
            "description": "",
            "depends_on": get_dashboard_depends_on(client, dashboard, model_map),
            "owner": {
                "name": first_owner["first_name"] + " " + first_owner["last_name"],
                "email": first_owner.get("email", "unknown"),
            },
        }
        exposures.append(exposure)

    with open(exposures_path, "w", encoding="utf-8") as output:
        yaml.safe_dump({"version": 2, "exposures": exposures}, output, sort_keys=False)
