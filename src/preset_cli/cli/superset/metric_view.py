"""
Export Superset datasets as Databricks Metric View YAML files.

A Metric View is a YAML-based semantic layer stored in Databricks Unity
Catalog, declaring dimensions and measures that can be queried through SQL,
Genie, or AI/BI Dashboards. This command converts the semantic information
already encoded in Superset datasets (calculated columns and metrics) into
that format, providing a starting point for migrating a Preset workspace to
Databricks.

See https://docs.databricks.com/aws/en/metric-views/ for the target schema.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.export import get_newline_char
from preset_cli.lib import split_comma

_logger = logging.getLogger(__name__)

METRIC_VIEW_VERSION = "0.1"

# Map common Superset d3 format strings to Databricks Metric View formats.
# Only emit a format when we are confident in the mapping; otherwise the user
# can edit the YAML directly. Patterns are intentionally narrow to avoid
# producing wrong output.
_D3_FORMAT_PATTERNS = (
    (re.compile(r"^,d$"), {"type": "number", "decimal_places": 0}),
    (re.compile(r"^,\.0f$"), {"type": "number", "decimal_places": 0}),
    (re.compile(r"^,\.(?P<p>\d+)f$"), {"type": "number"}),
    (re.compile(r"^\$,\.(?P<p>\d+)f$"), {"type": "currency", "currency_code": "USD"}),
    (re.compile(r"^\.(?P<p>\d+)%$"), {"type": "percentage"}),
)


def _translate_d3_format(d3format: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Translate a Superset d3 format string into a Metric View ``format`` block.

    Returns ``None`` for unrecognized patterns; the caller should skip the
    field rather than emit a guess.
    """
    if not d3format:
        return None

    for pattern, template in _D3_FORMAT_PATTERNS:
        match = pattern.match(d3format)
        if not match:
            continue
        result = dict(template)
        if "p" in match.groupdict():
            result["decimal_places"] = int(match.group("p"))
        return result

    return None


def _build_source(dataset: Dict[str, Any]) -> str:
    """
    Build the ``source`` value for a Metric View from a Superset dataset.

    Virtual datasets become a ``SELECT ...`` block; physical datasets become a
    fully qualified table reference using whatever catalog/schema metadata the
    dataset carries.
    """
    sql = dataset.get("sql")
    if sql:
        return sql.strip()

    parts: List[str] = []
    for key in ("catalog", "schema"):
        value = dataset.get(key)
        if value:
            parts.append(value)
    parts.append(dataset["table_name"])
    return ".".join(parts)


def _build_dimension(column: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Superset column entry into a Metric View dimension entry.
    """
    entry: Dict[str, Any] = {
        "name": column["column_name"],
        "expr": column.get("expression") or column["column_name"],
    }
    if column.get("verbose_name"):
        entry["display_name"] = column["verbose_name"]
    if column.get("description"):
        entry["comment"] = column["description"]
    return entry


def _build_measure(metric: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Superset metric entry into a Metric View measure entry.
    """
    entry: Dict[str, Any] = {
        "name": metric["metric_name"],
        "expr": metric["expression"],
    }
    if metric.get("verbose_name"):
        entry["display_name"] = metric["verbose_name"]
    if metric.get("description"):
        entry["comment"] = metric["description"]

    fmt = _translate_d3_format(metric.get("d3format"))
    if fmt is None and metric.get("currency"):
        fmt = {"type": "currency", "currency_code": metric["currency"]}
    if fmt is not None:
        entry["format"] = fmt

    return entry


def convert_dataset_to_metric_view(dataset: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Superset dataset payload into a Databricks Metric View dict.
    """
    metric_view: Dict[str, Any] = {
        "version": METRIC_VIEW_VERSION,
        "source": _build_source(dataset),
    }
    if dataset.get("description"):
        metric_view["comment"] = dataset["description"]

    dimensions = [
        _build_dimension(column)
        for column in dataset.get("columns", [])
        if column.get("is_active", True)
    ]
    if dimensions:
        metric_view["dimensions"] = dimensions

    measures = [_build_measure(metric) for metric in dataset.get("metrics", [])]
    if measures:
        metric_view["measures"] = measures

    return metric_view


def _safe_filename(dataset: Dict[str, Any]) -> str:
    """
    Derive a filename-safe stem for a dataset's Metric View YAML.
    """
    parts = [dataset.get("schema"), dataset["table_name"]]
    stem = "__".join(p for p in parts if p)
    return re.sub(r"[^A-Za-z0-9_.-]", "_", stem) + ".yaml"


@click.command()
@click.argument(
    "directory",
    type=click.Path(resolve_path=True),
    default="metric_views",
)
@click.option(
    "--dataset-ids",
    callback=split_comma,
    help="Comma separated list of dataset IDs to export (default: all)",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing files",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.pass_context
def export_metric_view(  # pylint: disable=too-many-locals
    ctx: click.core.Context,
    directory: str,
    dataset_ids: List[str],
    overwrite: bool = False,
    force_unix_eol: bool = False,
) -> None:
    """
    Export Superset datasets as Databricks Metric View YAML files.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)

    if dataset_ids:
        ids = [int(id_) for id_ in dataset_ids]
    else:
        ids = [dataset["id"] for dataset in client.get_datasets()]

    newline = get_newline_char(force_unix_eol)
    for dataset_id in ids:
        dataset = client.get_dataset(dataset_id)
        # ``get_dataset`` returns the resource payload directly via
        # ``get_resource``; some Superset versions wrap it in ``{"result": ...}``
        # but the client already unwraps it.
        metric_view = convert_dataset_to_metric_view(dataset)
        target = root / _safe_filename(dataset)
        if target.exists() and not overwrite:
            raise click.ClickException(
                f"File already exists and ``--overwrite`` was not specified: {target}",
            )
        with open(target, "w", encoding="utf-8", newline=newline) as output:
            yaml.dump(metric_view, output, sort_keys=False)
        click.echo(f"Wrote {target}")
