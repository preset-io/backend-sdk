"""
Commands to import RLS rules, ownership, and more.
"""

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.lib import (
    add_asset_to_log_dict,
    clean_logs,
    get_logs,
    write_logs_to_file,
)


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
def import_ownership(
    ctx: click.core.Context,
    path: str,
    continue_on_error: bool = False,
) -> None:
    """
    Import resource ownership from a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    logs = get_logs()
    failed_assets = (
        {log["uuid"] for log in logs["assets"] if log["status"] == "FAILED"}
        if logs.get("assets")
        else set()
    )

    # Remove FAILED logs to re-try them
    if "ownership" in logs:
        logs["ownership"] = [
            asset for asset in logs["ownership"] if asset["status"] != "FAILED"
        ]
    else:
        logs["ownership"] = []

    assets_to_skip = {log["uuid"] for log in logs["ownership"]} | failed_assets

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        for resource_name, resources in config.items():
            for ownership in resources:
                if ownership["uuid"] not in assets_to_skip:
                    try:
                        client.import_ownership(resource_name, ownership)
                    except Exception:  # pylint: disable=broad-except
                        if not continue_on_error:
                            write_logs_to_file(logs)
                            raise

                        add_asset_to_log_dict(
                            "ownership",
                            logs,
                            "FAILED",
                            ownership["uuid"],
                        )
                        continue

                    add_asset_to_log_dict(
                        "ownership",
                        logs,
                        "SUCCESS",
                        ownership["uuid"],
                    )

    if not continue_on_error or not any(
        log["status"] == "FAILED" for log in logs["ownership"]
    ):
        clean_logs("ownership", logs)
    else:
        write_logs_to_file(logs)
