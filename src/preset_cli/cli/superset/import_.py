"""
Commands to import RLS rules, ownership, and more.
"""

import logging

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.lib import (
    LogType,
    clean_logs,
    get_logs,
    write_logs_to_file,
)

_logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="rls.yaml",
)
@click.pass_context
def import_rls(ctx: click.core.Context, path: str) -> None:
    """
    Import RLS rules from a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        for rls in config:
            client.import_rls(rls)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="roles.yaml",
)
@click.pass_context
def import_roles(ctx: click.core.Context, path: str) -> None:
    """
    Import roles from a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        for role in config:
            client.import_role(role)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="ownership.yaml",
)
@click.option(
    "--continue-on-error",
    "-c",
    is_flag=True,
    default=False,
    help="Continue the import if an asset fails to import ownership",
)
@click.pass_context
def import_ownership(  # pylint: disable=too-many-locals
    ctx: click.core.Context,
    path: str,
    continue_on_error: bool = False,
) -> None:
    """
    Import resource ownership from a YAML file.
    """
    client = SupersetClient(baseurl=URL(ctx.obj["INSTANCE"]), auth=ctx.obj["AUTH"])

    log_file_path, logs = get_logs(LogType.OWNERSHIP)
    assets_to_skip = {log["uuid"] for log in logs[LogType.OWNERSHIP]} | {
        log["uuid"] for log in logs[LogType.ASSETS] if log["status"] == "FAILED"
    }

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)

    users = {user["email"]: user["id"] for user in client.export_users()}
    with open(log_file_path, "w", encoding="utf-8") as log_file:
        for resource_name, resources in config.items():
            resource_ids = {
                str(v): k for k, v in client.get_uuids(resource_name).items()
            }
            for ownership in resources:
                if ownership["uuid"] not in assets_to_skip:

                    _logger.info(
                        "Importing ownership for %s %s",
                        resource_name,
                        ownership["name"],
                    )
                    asset_log = {"uuid": ownership["uuid"], "status": "SUCCESS"}

                    try:
                        client.import_ownership(
                            resource_name,
                            ownership,
                            users,
                            resource_ids,
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        _logger.debug(
                            "Failed to import ownership for %s %s: %s",
                            resource_name,
                            ownership["name"],
                            str(exc),
                        )
                        if not continue_on_error:
                            raise
                        asset_log["status"] = "FAILED"

                    logs[LogType.OWNERSHIP].append(asset_log)
                    write_logs_to_file(log_file, logs)

    if not continue_on_error or not any(
        log["status"] == "FAILED" for log in logs[LogType.OWNERSHIP]
    ):
        clean_logs(LogType.OWNERSHIP, logs)
