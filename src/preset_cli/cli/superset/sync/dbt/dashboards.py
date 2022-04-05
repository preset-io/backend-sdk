"""
Sync Superset dashboards as DBT exposures.
"""

import json
from pathlib import Path
from typing import Any, List

import yaml

from preset_cli.api.clients.superset import SupersetClient

# XXX: DashboardResponseType and DatasetResponseType


def get_depends_on(client: SupersetClient, dashboard: Any) -> List[str]:
    """
    Get all the DBT dependencies for a given dashboard.
    """

    url = client.baseurl / "api/v1/dashboard" / str(dashboard["id"]) / "datasets"

    session = client.auth.get_session()
    headers = client.auth.get_headers()
    response = session.get(url, headers=headers)
    response.raise_for_status()

    payload = response.json()

    depends_on = []
    for dataset in payload["result"]:
        full_dataset = client.get_dataset(int(dataset["id"]))
        extra = json.loads(full_dataset["result"]["extra"] or "{}")
        if "depends_on" in extra:
            depends_on.append(extra["depends_on"])

    return depends_on


def sync_dashboards(  # pylint: disable=too-many-locals
    client: SupersetClient,
    exposures_path: Path,
    datasets: List[Any],
) -> None:
    """
    Write dashboards back to DBT as exposures.
    """
    dashboards_ids = set()

    for dataset in datasets:
        url = client.baseurl / "api/v1/dataset" / str(dataset["id"]) / "related_objects"

        session = client.auth.get_session()
        headers = client.auth.get_headers()
        response = session.get(url, headers=headers)
        response.raise_for_status()

        payload = response.json()
        for dashboard in payload["dashboards"]["result"]:
            dashboards_ids.add(dashboard["id"])

    exposures = []
    for dashboard_id in dashboards_ids:
        dashboards = client.get_dashboards(id=dashboard_id)
        if dashboards:
            dashboard = dashboards[0]
            first_owner = dashboard["owners"][0]
            exposure = {
                "name": dashboard["dashboard_title"],
                "type": "dashboard",
                "maturity": "high" if dashboard["published"] else "low",
                "url": str(client.baseurl / dashboard["url"].lstrip("/")),
                "description": "",
                "depends_on": get_depends_on(client, dashboard),
                "owner": {
                    "name": first_owner["first_name"] + " " + first_owner["last_name"],
                    "email": first_owner.get("email", "unknown"),
                },
            }
            exposures.append(exposure)

    with open(exposures_path, "w", encoding="utf-8") as output:
        yaml.safe_dump({"version": 2, "exposures": exposures}, output, sort_keys=False)
