"""
Backup and rollback helpers for delete assets.
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Set, Tuple
from zipfile import ZipFile

import click
import yaml

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset import dependency_utils as dep_utils
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_DASHBOARD,
    RESOURCE_DATABASE,
    RESOURCE_DATABASES,
    RESOURCE_DATASET,
    YAML_EXTENSIONS,
)
from preset_cli.cli.superset.delete_types import DashboardCascadeOptions
from preset_cli.lib import remove_root


def _verify_rollback_restoration(
    client: SupersetClient,
    backup_data: bytes,
    expected_types: Set[str],
) -> Tuple[List[str], List[str]]:
    """
    Verify rollback restored UUIDs for expected resource types.

    Returns:
      - list of unresolved resource types (cannot verify on this Superset version)
      - list of resource types with missing UUIDs
    """
    backup_uuids = dep_utils.extract_backup_uuids_by_type(backup_data)
    unresolved: List[str] = []
    missing_types: List[str] = []

    for resource_name in sorted(expected_types):
        expected_uuids = backup_uuids.get(resource_name, set())
        if not expected_uuids:
            continue

        _, _, missing_uuids, resolved = dep_utils.resolve_ids(
            client,
            resource_name,
            expected_uuids,
        )
        if not resolved:
            unresolved.append(resource_name)
            continue
        if missing_uuids:
            missing_types.append(resource_name)

    return unresolved, missing_types


def _parse_db_passwords(db_password: Tuple[str, ...]) -> Dict[str, str]:
    passwords: Dict[str, str] = {}
    for pair in db_password:
        if "=" not in pair:
            raise click.ClickException(
                "Invalid --db-password value. Use the format uuid=password.",
            )
        uuid, password = pair.split("=", 1)
        passwords[uuid] = password
    return passwords


def _write_backup(backup_data: bytes) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    with tempfile.NamedTemporaryFile(
        mode="wb",
        prefix=f"preset-cli-backup-delete-{timestamp}-",
        suffix=".zip",
        delete=False,
        dir=tempfile.gettempdir(),
    ) as output:
        output.write(backup_data)
        backup_path = Path(output.name)

    # Enforce private backup file permissions where supported.
    try:
        backup_path.chmod(0o600)
    except OSError:
        pass
    return str(backup_path)


def _apply_db_passwords_to_backup(
    backup_data: bytes,
    db_passwords: Dict[str, str],
) -> BytesIO:
    if not db_passwords:
        buf = BytesIO(backup_data)
        buf.seek(0)
        return buf

    input_buf = BytesIO(backup_data)
    output_buf = BytesIO()
    with ZipFile(input_buf) as src, ZipFile(output_buf, "w") as dest:
        for file_name in src.namelist():
            data = src.read(file_name)
            relative = remove_root(file_name)
            if relative.startswith(f"{RESOURCE_DATABASES}/") and file_name.endswith(
                YAML_EXTENSIONS,
            ):
                config = yaml.load(data, Loader=yaml.SafeLoader) or {}
                if uuid := config.get("uuid"):
                    if uuid in db_passwords:
                        config["password"] = db_passwords[uuid]
                        data = yaml.dump(config).encode()
            dest.writestr(file_name, data)
    output_buf.seek(0)
    return output_buf


def _rollback_deletion(
    client: SupersetClient,
    backup_data: bytes,
    import_resource_name: str,
    db_passwords: Dict[str, str],
    expected_types: Set[str],
) -> None:
    try:
        rollback_buf = _apply_db_passwords_to_backup(
            backup_data,
            db_passwords,
        )
        client.import_zip(import_resource_name, rollback_buf, overwrite=True)
    except Exception as exc:  # pylint: disable=broad-except
        click.echo(f"Rollback failed: {exc}")
        raise click.ClickException(
            "Rollback failed. A backup zip is available for manual restore.",
        ) from exc

    unresolved, missing_types = _verify_rollback_restoration(
        client,
        backup_data,
        expected_types,
    )
    if unresolved:
        labels = ", ".join(unresolved)
        click.echo(
            f"Warning: unable to verify rollback for: {labels}.",
        )
    if missing_types:
        labels = ", ".join(missing_types)
        raise click.ClickException(
            f"Rollback verification failed for: {labels}. "
            "A backup zip is available for manual restore.",
        )
    click.echo("Rollback succeeded.")


def _rollback_non_dashboard_deletion(
    client: SupersetClient,
    resource_name: str,
    backup_data: bytes,
    db_passwords: Dict[str, str],
) -> None:
    _rollback_deletion(
        client=client,
        backup_data=backup_data,
        import_resource_name=resource_name,
        db_passwords=(
            db_passwords if resource_name == RESOURCE_DATABASE else {}
        ),
        expected_types={resource_name},
    )


def _expected_dashboard_rollback_types(
    cascade_options: DashboardCascadeOptions,
) -> Set[str]:
    expected_types = {RESOURCE_DASHBOARD}
    if cascade_options.charts:
        expected_types.add(RESOURCE_CHART)
    if cascade_options.datasets:
        expected_types.add(RESOURCE_DATASET)
    if cascade_options.databases:
        expected_types.add(RESOURCE_DATABASE)
    return expected_types


def _rollback_dashboard_deletion(
    client: SupersetClient,
    backup_data: bytes,
    db_passwords: Dict[str, str],
    cascade_options: DashboardCascadeOptions,
) -> None:
    _rollback_deletion(
        client=client,
        backup_data=backup_data,
        import_resource_name=RESOURCE_DASHBOARD,
        db_passwords=db_passwords,
        expected_types=_expected_dashboard_rollback_types(cascade_options),
    )
