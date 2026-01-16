"""
Tests for the export commands.
"""

# pylint: disable=redefined-outer-name, invalid-name, unused-argument, too-many-lines

import json
from io import BytesIO
from pathlib import Path
from typing import Dict, List
from unittest import mock
from uuid import UUID
from zipfile import ZipFile

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.auth.main import Auth
from preset_cli.cli.superset.export import (
    build_local_uuid_mapping,
    check_asset_uniqueness,
    export_resource,
    extract_uuid_from_asset,
)
from preset_cli.cli.superset.main import superset_cli


@pytest.fixture
def chart_export() -> BytesIO:
    # pylint: disable=line-too-long
    """
    Fixture for the contents of a simple chart export.
    """
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/databases/gsheets.yaml": yaml.dump(
            {
                "database_name": "GSheets",
                "sqlalchemy_uri": "gsheets://",
            },
        ),
        "chart_export/datasets/gsheets/test.yaml": yaml.dump(
            {
                "table_name": "test",
                "sql": """
SELECT action, count(*) as times
FROM logs
{% if filter_values('action_type')|length %}
    WHERE action is null
    {% for action in filter_values('action_type') %}
        or action = '{{ action }}'
    {% endfor %}
{% endif %}
GROUP BY action""",
            },
        ),
        "chart_export/charts/test_01.yaml": yaml.dump(
            {
                "slice_name": "test",
                "viz_type": "big_number_total",
                "params": {
                    "datasource": "1__table",
                    "viz_type": "big_number_total",
                    "slice_id": 1,
                    "metric": {
                        "expressionType": "SQL",
                        "sqlExpression": "{% if from_dttm %} count(*) {% else %} count(*) {% endif %}",
                        "column": None,
                        "aggregate": None,
                        "datasourceWarning": False,
                        "hasCustomLabel": True,
                        "label": "custom_calculation",
                        "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
                    },
                    "adhoc_filters": [],
                    "header_font_size": 0.4,
                    "subheader_font_size": 0.15,
                    "y_axis_format": "SMART_NUMBER",
                    "time_format": "smart_date",
                    "extra_form_data": {},
                    "dashboards": [],
                },
                "query_context": """
{"datasource":{"id":1,"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},
"columns":[],"metrics":[{"expressionType":"SQL","sqlExpression":"{% if from_dttm %} count(*) {% else %} count(*) {% endif %}","column":null,"aggregate":null,
"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"}],"annotation_layers":[],
"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{}}],"form_data":{"datasource":"1__table","viz_type":"big_number_total",
"slice_id":1,"metric":{"expressionType":"SQL","sqlExpression":"{% if from_dttm %} count(*) {% else %} count(*) {% endif %}","column":null,"aggregate":null,
"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"},"adhoc_filters":[],"header_font_size":0.4,
"subheader_font_size":0.15,"y_axis_format":"SMART_NUMBER","time_format":"smart_date","extra_form_data":{},"dashboards":[],"force":false,"result_format":"json","result_type":"full"},
"result_format":"json","result_type":"full"}""",
            },
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)
    return buf


def test_export_resource(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    chart_export: BytesIO,
) -> None:
    # pylint: disable=line-too-long
    """
    Test ``export_resource``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = chart_export

    export_resource(
        resource_name="database",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )
    with open(root / "databases/gsheets.yaml", encoding="utf-8") as input_:
        assert input_.read() == "database_name: GSheets\nsqlalchemy_uri: gsheets://\n"

    client.get_resources.assert_called_once_with("database")

    # check that Jinja2 was escaped
    export_resource(
        resource_name="dataset",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )
    with open(root / "datasets/gsheets/test.yaml", encoding="utf-8") as input_:
        assert yaml.load(input_.read(), Loader=yaml.SafeLoader) == {
            "table_name": "test",
            "sql": """
SELECT action, count(*) as times
FROM logs
{{ '{% if' }} filter_values('action_type')|length {{ '%}' }}
    WHERE action is null
    {{ '{% for' }} action in filter_values('action_type') {{ '%}' }}
        or action = '{{ '{{' }} action {{ '}}' }}'
    {{ '{% endfor %}' }}
{{ '{% endif %}' }}
GROUP BY action""",
        }

    # check that chart JSON structure was not escaped, only Jinja templating
    export_resource(
        resource_name="chart",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )
    with open(root / "charts/test_01.yaml", encoding="utf-8") as input_:
        # load `query_context` as JSON to avoid
        # mismatches due to blank spaces, single vs double quotes, etc
        input__ = yaml.load(input_.read(), Loader=yaml.SafeLoader)
        print(input__["query_context"])
        input__["query_context"] = json.loads(
            input__["query_context"],
        )
        print(input__["query_context"])
        assert input__ == {
            "slice_name": "test",
            "viz_type": "big_number_total",
            "params": {
                "datasource": "1__table",
                "viz_type": "big_number_total",
                "slice_id": 1,
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
                    "column": None,
                    "aggregate": None,
                    "datasourceWarning": False,
                    "hasCustomLabel": True,
                    "label": "custom_calculation",
                    "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
                },
                "adhoc_filters": [],
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_format": "smart_date",
                "extra_form_data": {},
                "dashboards": [],
            },
            "query_context": json.loads(
                """
{"datasource":{"id":1,"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},"columns":[],
"metrics":[{"expressionType":"SQL","sqlExpression":"{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
"column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"}],
"annotation_layers":[],"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{}}],"form_data":{"datasource":"1__table",
"viz_type":"big_number_total","slice_id":1,"metric":{"expressionType":"SQL","sqlExpression":"{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
"column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"},
"adhoc_filters":[],"header_font_size":0.4,"subheader_font_size":0.15,"y_axis_format":"SMART_NUMBER","time_format":"smart_date",
"extra_form_data":{},"dashboards":[],"force":false,"result_format":"json","result_type":"full"},"result_format":"json","result_type":"full"}""",
            ),
        }

    # metadata file should be ignored
    assert not (root / "metadata.yaml").exists()


def test_export_resource_overwrite(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    chart_export: BytesIO,
) -> None:
    """
    Test that we need to confirm overwrites.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = chart_export

    export_resource(
        resource_name="database",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )
    with pytest.raises(Exception) as excinfo:
        export_resource(
            resource_name="database",
            requested_ids=set(),
            root=root,
            client=client,
            overwrite=False,
            disable_jinja_escaping=False,
            force_unix_eol=False,
        )
    assert str(excinfo.value) == (
        "File already exists and ``--overwrite`` was not specified: "
        "/path/to/root/databases/gsheets.yaml"
    )

    export_resource(
        resource_name="database",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=True,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )


def test_export_resource_with_ids(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    chart_export: BytesIO,
) -> None:
    """
    Test ``export_resource`` with ``requested_ids``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = chart_export

    export_resource(
        resource_name="database",
        requested_ids={1, 2, 3},
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
        force_unix_eol=False,
    )

    client.get_resources.assert_not_called()
    client.export_zip.assert_called_once_with("database", [1, 2, 3])


def test_export_assets(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_assets`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export", "/path/to/root"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "database",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
        ],
    )


def test_export_assets_by_id(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_assets`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export",
            "/path/to/root",
            "--database-ids",
            "1,2,3",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "database",
                {1, 2, 3},
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=False,
                force_unix_eol=False,
            ),
        ],
    )


def test_export_assets_by_type(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_assets`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export",
            "/path/to/root",
            "--asset-type",
            "dashboard",
            "--asset-type",
            "dataset",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
        ],
    )


def test_export_with_custom_auth(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export", "/path/to/root"],
        catch_exceptions=False,
        obj={"AUTH": Auth()},
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "database",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=False,
            ),
        ],
    )


def test_export_users(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_users`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-users", "users.yaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("users.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]


def test_export_users_force_unix_eol_enable(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_users`` command with ``--force-unix-eol`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-users",
            "users.yaml",
            "--force-unix-eol",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("users.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]


def test_export_roles(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_roles`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_roles.return_value = [
        {
            "name": "Public",
            "permissions": [],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-roles", "roles.yaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("roles.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "name": "Public",
            "permissions": [],
        },
    ]


def test_export_roles_force_unix_eol_enable(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_roles`` command with ``--force-unix-eol`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_roles.return_value = [
        {
            "name": "Public",
            "permissions": [],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-roles",
            "roles.yaml",
            "--force-unix-eol",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("roles.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "name": "Public",
            "permissions": [],
        },
    ]


def test_export_rls(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_rls`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_rls.return_value = [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-rls", "rls.yaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("rls.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]


def test_export_rls_force_unix_eol_enable(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_rls`` command with ``--force-unix-eol`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_rls.return_value = [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-rls", "rls.yaml", "--force-unix-eol"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("rls.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]


def test_export_ownership(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_ownership`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "adoe@example.com"},
        {"id": 2, "email": "bdoe@example.com"},
    ]
    client.export_ownership.side_effect = [
        [],
        [
            {
                "name": "My chart",
                "uuid": UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
        [],
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-ownership"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify export_ownership was called with users dict and exclude_old_users=False
    expected_users = {1: "adoe@example.com", 2: "bdoe@example.com"}
    assert client.export_ownership.call_count == 3
    client.export_ownership.assert_any_call("dataset", set(), expected_users, False)
    client.export_ownership.assert_any_call("chart", set(), expected_users, False)
    client.export_ownership.assert_any_call("dashboard", set(), expected_users, False)

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "chart": [
            {
                "name": "My chart",
                "uuid": "e0d20af0-cef9-4bdb-80b4-745827f441bf",
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
    }


def test_export_ownership_force_unix_eol_enable(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_ownership`` command with ``--force-unix-eol`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "adoe@example.com"},
        {"id": 2, "email": "bdoe@example.com"},
    ]
    client.export_ownership.side_effect = [
        [],
        [
            {
                "name": "My chart",
                "uuid": UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
        [],
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-ownership", "--force-unix-eol"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "chart": [
            {
                "name": "My chart",
                "uuid": "e0d20af0-cef9-4bdb-80b4-745827f441bf",
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
    }


def test_export_ownership_by_asset_type(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_ownership`` command with ``--asset-type`` filter.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "adoe@example.com"},
        {"id": 2, "email": "bdoe@example.com"},
    ]
    client.export_ownership.side_effect = [
        [
            {
                "name": "My chart",
                "uuid": UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
                "owners": ["adoe@example.com"],
            },
        ],
        [
            {
                "name": "My dashboard",
                "uuid": UUID("a1b2c3d4-1234-5678-90ab-cdefabcd1234"),
                "owners": ["bdoe@example.com"],
            },
        ],
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-ownership",
            "--asset-type",
            "chart",
            "--asset-type",
            "dashboard",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that export_ownership was called only for chart and dashboard, not dataset
    expected_users = {1: "adoe@example.com", 2: "bdoe@example.com"}
    assert client.export_ownership.call_count == 2
    client.export_ownership.assert_any_call("chart", set(), expected_users, False)
    client.export_ownership.assert_any_call("dashboard", set(), expected_users, False)

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "chart": [
            {
                "name": "My chart",
                "uuid": "e0d20af0-cef9-4bdb-80b4-745827f441bf",
                "owners": ["adoe@example.com"],
            },
        ],
        "dashboard": [
            {
                "name": "My dashboard",
                "uuid": "a1b2c3d4-1234-5678-90ab-cdefabcd1234",
                "owners": ["bdoe@example.com"],
            },
        ],
    }


def test_export_ownership_single_asset_type(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_ownership`` command with a single ``--asset-type``.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 3, "email": "cdoe@example.com"},
    ]
    client.export_ownership.return_value = [
        {
            "name": "My dataset",
            "uuid": UUID("12345678-1234-5678-90ab-cdefabcd5678"),
            "owners": ["cdoe@example.com"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-ownership",
            "--asset-type",
            "dataset",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that export_ownership was called only once for dataset
    expected_users = {3: "cdoe@example.com"}
    assert client.export_ownership.call_count == 1
    client.export_ownership.assert_called_once_with(
        "dataset",
        set(),
        expected_users,
        False,
    )

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "dataset": [
            {
                "name": "My dataset",
                "uuid": "12345678-1234-5678-90ab-cdefabcd5678",
                "owners": ["cdoe@example.com"],
            },
        ],
    }


def test_export_resource_jinja_escaping_disabled(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    chart_export: BytesIO,
) -> None:
    """
    Test ``export_resource`` with ``--disable-jinja-escaping``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = chart_export

    # check that Jinja2 was not escaped
    export_resource(
        resource_name="dataset",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=True,
        force_unix_eol=False,
    )
    with open(root / "datasets/gsheets/test.yaml", encoding="utf-8") as input_:
        assert yaml.load(input_.read(), Loader=yaml.SafeLoader) == {
            "table_name": "test",
            "sql": """
SELECT action, count(*) as times
FROM logs
{% if filter_values('action_type')|length %}
    WHERE action is null
    {% for action in filter_values('action_type') %}
        or action = '{{ action }}'
    {% endfor %}
{% endif %}
GROUP BY action""",
        }

    # metadata file should be ignored
    assert not (root / "metadata.yaml").exists()


def test_export_resource_jinja_escaping_disabled_command(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_assets`` with ``--disable-jinja-escaping`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export",
            "/path/to/root",
            "--disable-jinja-escaping",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "database",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
                force_unix_eol=False,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
                force_unix_eol=False,
            ),
        ],
    )


def test_export_resource_force_unix_eol_enabled(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    chart_export: BytesIO,
) -> None:
    """
    Test ``export_resource`` with ``--force-unix-eol`` flag
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = chart_export
    get_newline_char = mocker.patch("preset_cli.cli.superset.export.get_newline_char")
    get_newline_char.return_value = "\n"

    # check the newline char
    export_resource(
        resource_name="dataset",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=True,
        force_unix_eol=True,
    )

    get_newline_char.assert_called_once_with(True)


def test_export_resource_force_unix_eol_command(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_assets`` with ``--force-unix-eol`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export",
            "/path/to/root",
            "--force-unix-eol",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call(
                "database",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=True,
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=True,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=True,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
                force_unix_eol=True,
            ),
        ],
    )


def test_extract_uuid_from_asset() -> None:
    """
    Test the extract_uuid_from_asset helper function.
    """
    # Test valid YAML with UUID
    yaml_content = """
slice_name: Test Chart
uuid: 3f966611-8afc-4841-abdc-fa4361ff69f8
version: 1.0.0
"""
    assert (
        extract_uuid_from_asset(file_content=yaml_content)
        == "3f966611-8afc-4841-abdc-fa4361ff69f8"
    )

    # Test YAML without UUID
    yaml_content_no_uuid = """
slice_name: Test Chart
version: 1.0.0
"""
    assert extract_uuid_from_asset(file_content=yaml_content_no_uuid) is None


def test_build_local_uuid_mapping(fs: FakeFilesystem) -> None:
    """
    Test the build_local_uuid_mapping function.
    """
    root = Path("/test/root")

    # Create test directory structure with YAML files
    fs.create_dir(root / "dashboards")
    fs.create_dir(root / "charts")
    fs.create_dir(root / "datasets/db1")
    fs.create_dir(root / "databases")

    # Dashboard with UUID
    with open(root / "dashboards/dashboard1.yaml", "w", encoding="utf-8") as f:
        f.write("uuid: dashboard-uuid-1\nname: Dashboard 1")

    # Chart with UUID
    with open(root / "charts/chart1.yaml", "w", encoding="utf-8") as f:
        f.write("uuid: chart-uuid-1\nname: Chart 1")

    # Dataset with UUID (in subdirectory)
    with open(root / "datasets/db1/dataset1.yaml", "w", encoding="utf-8") as f:
        f.write("uuid: dataset-uuid-1\nname: Dataset 1")

    # Database with UUID
    with open(root / "databases/db1.yaml", "w", encoding="utf-8") as f:
        f.write("uuid: database-uuid-1\nname: Database 1")

    # File without UUID (should be ignored)
    with open(root / "charts/chart_no_uuid.yaml", "w", encoding="utf-8") as f:
        f.write("name: Chart without UUID")

    mapping = build_local_uuid_mapping(root)

    assert (
        mapping["dashboards"]["dashboard-uuid-1"] == root / "dashboards/dashboard1.yaml"
    )
    assert mapping["charts"]["chart-uuid-1"] == root / "charts/chart1.yaml"
    assert mapping["datasets"]["dataset-uuid-1"] == root / "datasets/db1/dataset1.yaml"
    assert mapping["databases"]["database-uuid-1"] == root / "databases/db1.yaml"
    assert "chart-no-uuid" not in mapping["charts"]


def test_export_resource_uuid_validation(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test export_resource with UUID-based overwrite validation.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create existing chart with UUID
    fs.create_dir(root / "charts")
    with open(root / "charts/old_name.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"slice_name": "Old Name", "uuid": "same-uuid-123"}))

    # Create ZIP with chart that has the same UUID but different name
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/charts/new_name.yaml": yaml.dump(
            {"slice_name": "New Name", "uuid": "same-uuid-123"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Should raise exception without overwrite flag
    with pytest.raises(Exception) as excinfo:
        export_resource(
            resource_name="chart",
            requested_ids=set(),
            root=root,
            client=client,
            overwrite=False,
            disable_jinja_escaping=True,
            force_unix_eol=False,
        )
    assert "Resource with UUID same-uuid-123 already exists" in str(excinfo.value)
    assert "old_name.yaml" in str(excinfo.value)


def test_export_resource_uuid_with_overwrite(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test export_resource with UUID-based overwrite enabled.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create existing chart with UUID
    fs.create_dir(root / "charts")
    with open(root / "charts/old_name.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"slice_name": "Old Name", "uuid": "same-uuid-123"}))

    # Create ZIP with chart that has the same UUID but different name
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/charts/new_name.yaml": yaml.dump(
            {"slice_name": "New Name", "uuid": "same-uuid-123"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Should succeed with overwrite flag and delete old file
    export_resource(
        resource_name="chart",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=True,
        disable_jinja_escaping=True,
        force_unix_eol=False,
    )

    # Old file should be deleted
    assert not (root / "charts/old_name.yaml").exists()
    # New file should exist
    assert (root / "charts/new_name.yaml").exists()

    with open(root / "charts/new_name.yaml", encoding="utf-8") as f:
        content = yaml.load(f.read(), Loader=yaml.SafeLoader)
        assert content["slice_name"] == "New Name"
        assert content["uuid"] == "same-uuid-123"


def test_export_resource_uuid_with_overrides(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test that multiple resources with same UUID are handled correctly.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create existing dashboards with different UUIDs
    fs.create_dir(root / "dashboards")
    with open(root / "dashboards/dashboard1.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"dashboard_title": "Dashboard 1", "uuid": "uuid-1"}))
    with open(root / "dashboards/dashboard2.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"dashboard_title": "Dashboard 2", "uuid": "uuid-2"}))

    # Create ZIP with renamed dashboards (same UUIDs, different names)
    contents = {
        "dashboard_export/metadata.yaml": "Metadata",
        "dashboard_export/dashboards/renamed_dashboard1.yaml": yaml.dump(
            {"dashboard_title": "Renamed Dashboard 1", "uuid": "uuid-1"},
        ),
        "dashboard_export/dashboards/renamed_dashboard2.yaml": yaml.dump(
            {"dashboard_title": "Renamed Dashboard 2", "uuid": "uuid-2"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Should succeed with overwrite flag and delete both old files
    export_resource(
        resource_name="dashboard",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=True,
        disable_jinja_escaping=True,
        force_unix_eol=False,
    )

    # Old files should be deleted
    assert not (root / "dashboards/dashboard1.yaml").exists()
    assert not (root / "dashboards/dashboard2.yaml").exists()
    # New files should exist
    assert (root / "dashboards/renamed_dashboard1.yaml").exists()
    assert (root / "dashboards/renamed_dashboard2.yaml").exists()


def test_export_resource_backward_compatibility(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test that export still works for resources without UUID fields.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create ZIP with resources without UUID
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/charts/chart_no_uuid.yaml": yaml.dump(
            {"slice_name": "Chart without UUID"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Should succeed for resources without UUID
    export_resource(
        resource_name="chart",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=True,
        force_unix_eol=False,
    )

    assert (root / "charts/chart_no_uuid.yaml").exists()

    # Second export of same file without UUID should fail without overwrite
    buf.seek(0)
    client.export_zip.return_value = buf

    with pytest.raises(Exception) as excinfo:
        export_resource(
            resource_name="chart",
            requested_ids=set(),
            root=root,
            client=client,
            overwrite=False,
            disable_jinja_escaping=True,
            force_unix_eol=False,
        )
    assert "File already exists" in str(excinfo.value)


def test_extract_uuid_from_asset_file_path(fs: FakeFilesystem) -> None:
    """
    Test extract_uuid_from_asset with file path parameter.
    """
    # Test with file path
    fs.create_dir("/test")
    test_file = Path("/test/chart.yaml")
    with open(test_file, "w", encoding="utf-8") as f:
        f.write("slice_name: Test\nuuid: test-uuid-123")

    assert extract_uuid_from_asset(file_path=test_file) == "test-uuid-123"

    # Test with empty file
    empty_file = Path("/test/empty.yaml")
    with open(empty_file, "w", encoding="utf-8") as f:
        f.write("")
    assert extract_uuid_from_asset(file_path=empty_file) is None


def test_check_asset_uniqueness_no_uuid(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test check_asset_uniqueness with file content that has no UUID.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create empty UUID mapping
    uuid_mapping: Dict[str, Dict[str, Path]] = {
        "charts": {},
        "dashboards": {},
        "datasets": {},
        "databases": {},
    }
    files_to_delete: List[Path] = []

    # File content without UUID
    file_content = yaml.dump({"slice_name": "Test Chart"})
    target = root / "charts/test.yaml"

    # Should return early without doing anything
    check_asset_uniqueness(
        overwrite=False,
        file_content=file_content,
        file_name="charts/test.yaml",
        file_path=target,
        uuid_mapping=uuid_mapping,
        files_to_delete=files_to_delete,
    )

    # No files should be added to deletion queue
    assert len(files_to_delete) == 0


def test_check_asset_uniqueness_unknown_resource_type(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test check_asset_uniqueness with unknown resource type.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create empty UUID mapping
    uuid_mapping: Dict[str, Dict[str, Path]] = {
        "charts": {},
        "dashboards": {},
        "datasets": {},
        "databases": {},
    }
    files_to_delete: List[Path] = []

    # File content with UUID but unknown resource type
    file_content = yaml.dump({"slice_name": "Test Chart", "uuid": "test-uuid"})
    target = root / "unknown/test.yaml"

    # Should return early without doing anything
    check_asset_uniqueness(
        overwrite=False,
        file_content=file_content,
        file_name="unknown/test.yaml",  # Unknown resource type
        file_path=target,
        uuid_mapping=uuid_mapping,
        files_to_delete=files_to_delete,
    )

    # No files should be added to deletion queue
    assert len(files_to_delete) == 0


def test_check_asset_uniqueness_uuid_conflict_no_overwrite() -> None:
    """
    Test check_asset_uniqueness when UUID conflict exists but overwrite is False.
    """
    root = Path("/test/root")

    # Create UUID mapping with existing UUID
    existing_file = root / "charts/existing.yaml"
    uuid_mapping: Dict[str, Dict[str, Path]] = {
        "charts": {"conflict-uuid": existing_file},
        "dashboards": {},
        "datasets": {},
        "databases": {},
    }
    files_to_delete: List[Path] = []

    # File content with conflicting UUID
    file_content = yaml.dump({"slice_name": "New Chart", "uuid": "conflict-uuid"})
    target = root / "charts/new.yaml"

    # Should raise exception when overwrite=False and UUID conflicts
    with pytest.raises(Exception) as excinfo:
        check_asset_uniqueness(
            overwrite=False,
            file_content=file_content,
            file_name="charts/new.yaml",
            file_path=target,
            uuid_mapping=uuid_mapping,
            files_to_delete=files_to_delete,
        )

    assert "Resource with UUID conflict-uuid already exists" in str(excinfo.value)
    assert "existing.yaml" in str(excinfo.value)
    assert "Use --overwrite flag" in str(excinfo.value)


def test_build_local_uuid_mapping_with_db_subdirs(fs: FakeFilesystem) -> None:
    """
    Test build_local_uuid_mapping handles dataset subdirectories correctly.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create datasets with database subdirectories
    fs.create_dir(root / "datasets/postgres")
    fs.create_dir(root / "datasets/mysql")

    # IMPORTANT: Create a file (not directory) in datasets to test line 67
    with open(root / "datasets/not_a_dir.txt", "w", encoding="utf-8") as f:
        f.write("This is not a directory")

    # Dataset with UUID in postgres subdir
    with open(root / "datasets/postgres/table1.yaml", "w", encoding="utf-8") as f:
        f.write("table_name: table1\nuuid: postgres-table-uuid")

    # Dataset with UUID in mysql subdir
    with open(root / "datasets/mysql/table2.yaml", "w", encoding="utf-8") as f:
        f.write("table_name: table2\nuuid: mysql-table-uuid")

    # Dataset without UUID
    with open(
        root / "datasets/postgres/no_uuid_table.yaml",
        "w",
        encoding="utf-8",
    ) as f:
        f.write("table_name: no_uuid_table")

    mapping = build_local_uuid_mapping(root)

    assert (
        mapping["datasets"]["postgres-table-uuid"]
        == root / "datasets/postgres/table1.yaml"
    )
    assert (
        mapping["datasets"]["mysql-table-uuid"] == root / "datasets/mysql/table2.yaml"
    )
    assert "no-uuid" not in mapping["datasets"]


def test_check_asset_uniqueness_all_resource_types() -> None:
    """
    Test check_asset_uniqueness with all resource types to ensure full coverage.
    """
    root = Path("/test/root")

    # Test with databases resource type
    uuid_mapping: Dict[str, Dict[str, Path]] = {
        "charts": {},
        "dashboards": {},
        "datasets": {},
        "databases": {"db-uuid": root / "databases/existing_db.yaml"},
    }
    files_to_delete: List[Path] = []

    # Test databases conflict
    file_content = yaml.dump({"database_name": "New DB", "uuid": "db-uuid"})
    target = root / "databases/new_db.yaml"

    with pytest.raises(Exception) as excinfo:
        check_asset_uniqueness(
            overwrite=False,
            file_content=file_content,
            file_name="databases/new_db.yaml",
            file_path=target,
            uuid_mapping=uuid_mapping,
            files_to_delete=files_to_delete,
        )

    assert "Resource with UUID db-uuid already exists" in str(excinfo.value)

    # Test with overwrite=True for databases
    check_asset_uniqueness(
        overwrite=True,
        file_content=file_content,
        file_name="databases/new_db.yaml",
        file_path=target,
        uuid_mapping=uuid_mapping,
        files_to_delete=files_to_delete,
    )

    # Should have added the existing file to deletion queue
    assert len(files_to_delete) == 1
    assert files_to_delete[0] == root / "databases/existing_db.yaml"

    # Test datasets type as well
    uuid_mapping = {
        "charts": {},
        "dashboards": {},
        "datasets": {"dataset-uuid": root / "datasets/postgres/existing.yaml"},
        "databases": {},
    }
    files_to_delete = []

    file_content = yaml.dump({"table_name": "New Table", "uuid": "dataset-uuid"})
    target = root / "datasets/mysql/new_table.yaml"

    check_asset_uniqueness(
        overwrite=True,
        file_content=file_content,
        file_name="datasets/mysql/new_table.yaml",
        file_path=target,
        uuid_mapping=uuid_mapping,
        files_to_delete=files_to_delete,
    )

    assert len(files_to_delete) == 1
    assert files_to_delete[0] == root / "datasets/postgres/existing.yaml"


def test_export_resource_deletion_failure_with_unlink(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test export_resource when file deletion with unlink fails.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create existing chart with UUID
    fs.create_dir(root / "charts")
    with open(root / "charts/old_name.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"slice_name": "Old Name", "uuid": "same-uuid-123"}))

    # Create ZIP with chart that has the same UUID but different name
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/charts/new_name.yaml": yaml.dump(
            {"slice_name": "New Name", "uuid": "same-uuid-123"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Mock unlink to raise an error
    def mock_unlink(self):
        if str(self).endswith("old_name.yaml"):
            raise OSError("Permission denied")

    # Patch the unlink method directly on pathlib.Path
    mocker.patch("pathlib.Path.unlink", mock_unlink)
    mock_echo = mocker.patch("preset_cli.cli.superset.export.click.echo")

    # Should raise SystemExit(1) when deletion fails
    with pytest.raises(SystemExit) as excinfo:
        export_resource(
            resource_name="chart",
            requested_ids=set(),
            root=root,
            client=client,
            overwrite=True,
            disable_jinja_escaping=True,
            force_unix_eol=False,
        )

    assert excinfo.value.code == 1
    mock_echo.assert_called_once()
    error_msg = mock_echo.call_args[0][0]
    assert "Failed to delete the following files:" in error_msg
    assert "old_name.yaml" in error_msg


def test_export_resource_mixed_uuid_conflicts(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test export with both UUID conflicts and regular file conflicts.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create existing files
    fs.create_dir(root / "charts")
    # File with UUID that will conflict
    with open(root / "charts/old_uuid_file.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"slice_name": "Old UUID File", "uuid": "conflict-uuid"}))

    # File without UUID that will conflict by name
    with open(root / "charts/same_name.yaml", "w", encoding="utf-8") as f:
        f.write(yaml.dump({"slice_name": "Same Name"}))

    # Create ZIP with conflicting files
    contents = {
        "chart_export/metadata.yaml": "Metadata",
        "chart_export/charts/new_uuid_file.yaml": yaml.dump(
            {"slice_name": "New UUID File", "uuid": "conflict-uuid"},
        ),
        "chart_export/charts/same_name.yaml": yaml.dump(
            {"slice_name": "Same Name Updated"},
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)

    client = mocker.MagicMock()
    client.get_resources.return_value = []
    client.export_zip.return_value = buf

    # Should succeed with overwrite flag
    export_resource(
        resource_name="chart",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=True,
        disable_jinja_escaping=True,
        force_unix_eol=False,
    )

    # UUID conflict: old file should be deleted, new file created
    assert not (root / "charts/old_uuid_file.yaml").exists()
    assert (root / "charts/new_uuid_file.yaml").exists()

    # Name conflict: file should be overwritten
    assert (root / "charts/same_name.yaml").exists()
    with open(root / "charts/same_name.yaml", encoding="utf-8") as f:
        content = yaml.load(f.read(), Loader=yaml.SafeLoader)
        assert content["slice_name"] == "Same Name Updated"


def test_build_local_uuid_mapping_empty_dirs(fs: FakeFilesystem) -> None:
    """
    Test build_local_uuid_mapping with empty or non-existent directories.
    """
    root = Path("/test/root")
    fs.create_dir(root)

    # Create empty directories
    fs.create_dir(root / "dashboards")
    fs.create_dir(root / "charts")
    # Don't create datasets and databases dirs

    mapping = build_local_uuid_mapping(root)

    # Should return empty mappings for all resource types
    assert mapping["dashboards"] == {}
    assert mapping["charts"] == {}
    assert mapping["datasets"] == {}
    assert mapping["databases"] == {}


def test_export_ownership_by_ids(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_ownership`` command with ID filters.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "adoe@example.com"},
    ]
    client.export_ownership.side_effect = [
        [
            {
                "name": "Dataset 1",
                "uuid": UUID("11111111-1111-1111-1111-111111111111"),
                "owners": ["adoe@example.com"],
            },
        ],
        [
            {
                "name": "Chart 5",
                "uuid": UUID("55555555-5555-5555-5555-555555555555"),
                "owners": ["adoe@example.com"],
            },
        ],
        [],
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-ownership",
            "--dataset-ids",
            "1,2",
            "--chart-ids",
            "5",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    expected_users = {1: "adoe@example.com"}
    assert client.export_ownership.call_count == 2
    client.export_ownership.assert_any_call("dataset", {1, 2}, expected_users, False)
    client.export_ownership.assert_any_call("chart", {5}, expected_users, False)

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "dataset": [
            {
                "name": "Dataset 1",
                "uuid": "11111111-1111-1111-1111-111111111111",
                "owners": ["adoe@example.com"],
            },
        ],
        "chart": [
            {
                "name": "Chart 5",
                "uuid": "55555555-5555-5555-5555-555555555555",
                "owners": ["adoe@example.com"],
            },
        ],
    }


def test_export_ownership_by_ids_and_asset_type(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``export_ownership`` command with both ID and asset-type filters.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "adoe@example.com"},
    ]
    client.export_ownership.return_value = [
        {
            "name": "Dashboard 10",
            "uuid": UUID("10101010-1010-1010-1010-101010101010"),
            "owners": ["adoe@example.com"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-ownership",
            "--asset-type",
            "dashboard",
            "--dashboard-ids",
            "10,20",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    expected_users = {1: "adoe@example.com"}
    assert client.export_ownership.call_count == 1
    client.export_ownership.assert_called_once_with(
        "dashboard",
        {10, 20},
        expected_users,
        False,
    )

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "dashboard": [
            {
                "name": "Dashboard 10",
                "uuid": "10101010-1010-1010-1010-101010101010",
                "owners": ["adoe@example.com"],
            },
        ],
    }
