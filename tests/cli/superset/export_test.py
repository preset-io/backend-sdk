"""
Tests for the export commands.
"""
# pylint: disable=redefined-outer-name, invalid-name, unused-argument

import json
from io import BytesIO
from pathlib import Path
from unittest import mock
from uuid import UUID
from zipfile import ZipFile

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.auth.main import Auth
from preset_cli.cli.superset.export import export_resource
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
    )
    with open(root / "databases/gsheets.yaml", encoding="utf-8") as input_:
        assert input_.read() == "database_name: GSheets\nsqlalchemy_uri: gsheets://\n"

    # check that Jinja2 was escaped
    export_resource(
        resource_name="dataset",
        requested_ids=set(),
        root=root,
        client=client,
        overwrite=False,
        disable_jinja_escaping=False,
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
    )
    with pytest.raises(Exception) as excinfo:
        export_resource(
            resource_name="database",
            requested_ids=set(),
            root=root,
            client=client,
            overwrite=False,
            disable_jinja_escaping=False,
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
    )


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
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
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
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
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
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                False,
                skip_related=True,
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


def test_export_ownership(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_ownership`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
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
            ),
            mock.call(
                "dataset",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
            ),
            mock.call(
                "chart",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
            ),
            mock.call(
                "dashboard",
                set(),
                Path("/path/to/root"),
                client,
                False,
                True,
                skip_related=True,
            ),
        ],
    )
