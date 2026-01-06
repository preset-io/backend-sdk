"""
Helper functions for DJ sync.
"""

import json
from typing import Any, Optional
from uuid import UUID

from datajunction import DJClient  # pylint: disable=no-name-in-module
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany


def sync_cube(  # pylint: disable=too-many-arguments
    database_uuid: UUID,
    schema: str,
    dj_client: DJClient,
    superset_client: SupersetClient,
    cube: str,
    base_url: Optional[URL],
) -> None:
    """
    Sync a DJ cube to a Superset virtual dataset.
    """
    response = dj_client._session.post(  # pylint: disable=protected-access
        "/graphql",
        json={
            "query": """
query FindCubes($names:[String!], $tags: [String!]) {
  findNodes(names: $names, tags: $tags, nodeTypes: [CUBE]) {
    name
    current {
      description
      displayName
      cubeMetrics {
        name
        description
        extractedMeasures {
          derivedExpression
        }
      }
      cubeDimensions {
        name
      }
    }
  }
}
            """,
            "variables": {"names": [cube]},
        },
    )
    payload = response.json()
    description = payload["data"]["findNodes"][0]["current"]["description"]
    columns = [
        dimension["name"]
        for dimension in payload["data"]["findNodes"][0]["current"]["cubeDimensions"]
    ]
    metrics = [
        {
            "metric_name": metric["name"],
            "expression": metric["extractedMeasures"]["derivedExpression"],
            "description": metric["description"],
        }
        for metric in payload["data"]["findNodes"][0]["current"]["cubeMetrics"]
    ]

    response = dj_client._session.post(  # pylint: disable=protected-access
        "/graphql",
        json={
            "query": """
query MeasuresSql($metrics: [String!]!, $dimensions: [String!]!) {
  measuresSql(
    cube: {metrics: $metrics, dimensions: $dimensions, filters: []}
    preaggregate: true
  ) {
    sql
  }
}
            """,
            "variables": {
                "metrics": [metric["metric_name"] for metric in metrics],
                "dimensions": columns,
            },
        },
    )
    payload = response.json()
    sql = payload["data"]["measuresSql"][0]["sql"]

    database = get_database(superset_client, database_uuid)
    dataset = get_or_create_dataset(superset_client, database, schema, cube, sql)

    superset_client.update_dataset(
        dataset["id"],
        override_columns=True,
        metrics=[],
    )

    superset_client.update_dataset(
        dataset["id"],
        override_columns=False,
        metrics=metrics,
        description=description,
        is_managed_externally=True,
        external_url=base_url / "nodes" / cube if base_url else None,
        extra=json.dumps(
            {
                "certification": {
                    "certified_by": "DJ",
                    "details": "This table is created by DJ.",
                },
            },
        ),
        sql=sql,
    )


def get_database(superset_client: SupersetClient, uuid: UUID) -> dict[str, Any]:
    """
    Get database info given its UUID.
    """
    databases = superset_client.get_databases(uuid=str(uuid))
    if not databases:
        raise ValueError(f"Database with UUID {uuid} not found in Superset.")

    return databases[0]


def get_or_create_dataset(
    superset_client: SupersetClient,
    database: dict[str, Any],
    schema: str,
    cube: str,
    sql: str,
) -> dict[str, Any]:
    """
    Get or create a dataset in Superset.
    """
    if existing := superset_client.get_datasets(
        database=OneToMany(database["id"]),  # type: ignore
        schema=schema,
        table_name=cube,
    ):
        dataset = existing[0]
        return superset_client.get_dataset(dataset["id"])

    return superset_client.create_dataset(
        database=database["id"],
        catalog=None,
        schema=schema,
        table_name=cube,
        sql=sql,
    )
