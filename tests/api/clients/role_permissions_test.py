"""Dataset identity and permission-set regressions for role migrations."""

# pylint: disable=protected-access

import pytest
import yaml
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from preset_cli.api.clients.superset import (
    RoleType,
    SupersetClient,
    format_dataset_permission,
    parse_dataset_permission,
    resolve_role_permissions,
)
from preset_cli.auth.main import Auth
from preset_cli.exceptions import SupersetError


@pytest.mark.parametrize("missing_endpoint", ["permissions-resources", "roles/search"])
def test_security_api_detection_404_uses_legacy(
    mocker: MockerFixture,
    requests_mock: Mocker,
    missing_endpoint: str,
) -> None:
    """Superset's search can exist while the FAB security APIs are disabled."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    mocker.patch.object(client, "get_users", return_value=[])
    search = requests_mock.get(
        base + "api/v1/security/roles/search/",
        json={"result": []},
    )
    requests_mock.get(
        base + "api/v1/security/permissions-resources/",
        json={"result": []},
    )
    requests_mock.get(base + f"api/v1/security/{missing_endpoint}/", status_code=404)
    requests_mock.get(base + "roles/add", text='<select id="permissions"></select>')
    requests_mock.get(base + "roles/list/", text="<table></table>")
    created = requests_mock.post(base + "roles/add")

    client.import_role(
        {"name": "Analyst", "permissions": [], "users": [], "groups": []},
    )

    assert created.call_count == 1
    if missing_endpoint == "permissions-resources":
        assert not search.called


@pytest.mark.parametrize("operation", ["import", "export"])
@pytest.mark.parametrize("endpoint", ["permissions-resources", "roles/search"])
@pytest.mark.parametrize("status_code", [401, 403, 500, 503])
def test_security_api_errors_do_not_trigger_legacy(
    mocker: MockerFixture,
    requests_mock: Mocker,
    operation: str,
    endpoint: str,
    status_code: int,
) -> None:
    """Authentication and server failures must retain their original HTTP status."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    mocker.patch.object(client, "get_users", return_value=[])
    for resource in ("permissions-resources", "roles/search", "groups"):
        requests_mock.get(base + f"api/v1/security/{resource}/", json={"result": []})
    requests_mock.get(
        base + f"api/v1/security/{endpoint}/",
        status_code=status_code,
        json={"message": "Security API failed"},
    )

    with pytest.raises(SupersetError) as exc:
        if operation == "import":
            client.import_role(
                {"name": "Analyst", "permissions": [], "users": [], "groups": []},
            )
        else:
            client.export_roles()

    assert exc.value.status_code == status_code
    assert all(
        request.method == "GET" and request.path.startswith("/api/v1/")
        for request in requests_mock.request_history
    )


@pytest.mark.parametrize(
    "method, endpoint",
    [
        ("POST", "roles/"),
        ("POST", "roles/42/permissions"),
        ("PUT", "roles/42/users"),
        ("PUT", "roles/42/groups"),
    ],
)
def test_role_write_404_never_retries_using_legacy(
    mocker: MockerFixture,
    requests_mock: Mocker,
    method: str,
    endpoint: str,
) -> None:
    """A missing write endpoint is a failure, not permission to retry via HTML."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    mocker.patch.object(client, "get_users", return_value=[])
    legacy = mocker.patch.object(client, "_import_role_legacy")
    for resource in ("permissions-resources", "roles/search", "groups"):
        requests_mock.get(base + f"api/v1/security/{resource}/", json={"result": []})
    requests_mock.post(base + "api/v1/security/roles/", json={"id": 42})
    requests_mock.post(base + "api/v1/security/roles/42/permissions", json={})
    requests_mock.put(base + "api/v1/security/roles/42/users", json={})
    requests_mock.put(base + "api/v1/security/roles/42/groups", json={})
    requests_mock.register_uri(
        method,
        base + f"api/v1/security/{endpoint}",
        status_code=404,
    )

    with pytest.raises(SupersetError) as exc:
        client.import_role(
            {"name": "Analyst", "permissions": [], "users": [], "groups": []},
        )

    assert exc.value.status_code == 404
    legacy.assert_not_called()
    assert (
        sum(
            "/permissions-resources/" in request.path
            for request in requests_mock.request_history
        )
        == 1
    )


def test_resolved_permission_ids_are_deduplicated() -> None:
    """Two source IDs can legitimately collapse onto one target permission."""
    assert resolve_role_permissions(
        [
            "datasource access on [db].[orders](id:1)",
            "database access on [db].(id:3)",
            "datasource access on [db].[orders](id:2)",
            "database access on [db].(id:4)",
        ],
        [
            ("datasource access on [db].[orders](id:99)", 50),
            ("database access on [db].(id:88)", 9),
        ],
    ) == [50, 9]


@pytest.mark.parametrize(
    "permission",
    [
        "schema access on [warehouse].[public]",
        "schema access on [warehouse].[analytics].[public]",
    ],
)
def test_exact_schema_grants_remain_supported(permission: str) -> None:
    """Both native schema formats work when the target has the exact grant."""
    assert resolve_role_permissions([permission], [(permission, 501)]) == [501]


@pytest.mark.parametrize("backend", ["superset", "preset", "legacy"])
@pytest.mark.parametrize(
    "permission",
    [
        "database access on [Missing].(id:1)",
        "datasource access on [Missing].[orders](id:1)",
        "dataset access on [Missing].[catalog].[public].[orders]",
        "all query access on all_query_access",
    ],
)
def test_unmatched_permissions_fail_before_role_writes(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
    permission: str,
) -> None:
    """A missing grant must not be silently dropped on any import path."""
    base = (
        "https://abc.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    client = SupersetClient(base, Auth())
    if backend == "preset":
        requests_mock.get(base + "api/v1/security/dar/", json={"result": []})
    mocker.patch.object(client, "get_users", return_value=[])
    if backend == "legacy":
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            status_code=404,
        )
        requests_mock.get(base + "roles/add", text='<select id="permissions"></select>')
    else:
        requests_mock.get(base + "api/v1/security/roles/search/", json={"result": []})
        endpoint = "dar_permissions" if backend == "preset" else "permissions-resources"
        requests_mock.get(base + f"api/v1/security/{endpoint}/", json={"result": []})

    with pytest.raises(ValueError, match="not found in target"):
        client.import_role(
            {"name": "Analyst", "permissions": [permission], "users": [], "groups": []},
        )
    assert all(request.method == "GET" for request in requests_mock.request_history)


@pytest.mark.parametrize("backend", ["superset", "preset", "legacy"])
@pytest.mark.parametrize("relationship", ["users", "groups"])
def test_missing_memberships_prevent_writes(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
    relationship: str,
) -> None:
    """All backends reject memberships they cannot apply, before writing."""
    base = (
        "https://abc.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    client = SupersetClient(base, Auth())
    mocker.patch.object(client, "get_users", return_value=[])
    for resource in (
        "permissions-resources",
        "roles/search",
        "dar_permissions",
        "dar",
        "groups",
    ):
        requests_mock.get(base + f"api/v1/security/{resource}/", json={"result": []})
    if backend == "legacy":
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            status_code=404,
        )
        requests_mock.get(base + "roles/add", text='<select id="permissions"></select>')
    users = ["missing@example.com"] if relationship == "users" else []
    groups = ["Analysts"] if relationship == "groups" else []
    if users:
        error = "User missing@example.com not found in target"
    elif backend == "legacy":
        error = "Legacy Superset role imports do not support groups"
    else:
        error = "Group Analysts not found in target"

    with pytest.raises(ValueError, match=error):
        client.import_role(
            {"name": "Analyst", "permissions": [], "users": users, "groups": groups},
        )
    assert all(request.method == "GET" for request in requests_mock.request_history)


@pytest.mark.parametrize("backend", ["superset", "preset", "legacy"])
@pytest.mark.parametrize(
    "source_path, target_path",
    [
        ("[warehouse].[public]", "[warehouse].[analytics].[public]"),
        ("[warehouse].[analytics].[public]", "[warehouse].[public]"),
        ("[warehouse].[analytics].[public]", "[warehouse].[other].[public]"),
        ("[warehouse].[public]", None),
    ],
)
def test_unmatched_schema_grants_fail_before_role_writes(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
    source_path: str,
    target_path: str | None,
) -> None:
    """Do not write even the matching grants when a schema cannot be resolved."""
    base = (
        "https://abc.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    client = SupersetClient(base, Auth())
    if backend == "preset":
        requests_mock.get(base + "api/v1/security/dar/", json={"result": []})
    mocker.patch.object(client, "get_users", return_value=[])
    target: list[dict] = [
        {
            "id": 500,
            "permission": "database_access",
            "view_menu": "[warehouse].(id:99)",
        },
    ]
    if target_path is not None:
        target.append(
            {"id": 501, "permission": "schema_access", "view_menu": target_path},
        )
    if backend == "legacy":
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            status_code=404,
        )
        options = "".join(
            f'<option value="{p["id"]}">'
            f'{p["permission"].replace("_", " ")} on {p["view_menu"]}</option>'
            for p in target
        )
        requests_mock.get(
            base + "roles/add",
            text=f'<select id="permissions">{options}</select>',
        )
    else:
        if backend == "superset":
            requests_mock.get(
                base + "api/v1/security/roles/search/",
                [
                    {"json": {"result": [{"id": 42, "name": "Analyst"}]}},
                    {"json": {"result": []}},
                ],
            )
            target = [
                {
                    "id": p["id"],
                    "permission": {"name": p["permission"]},
                    "view_menu": {"name": p["view_menu"]},
                }
                for p in target
            ]
        endpoint = "dar_permissions" if backend == "preset" else "permissions-resources"
        requests_mock.get(
            base + f"api/v1/security/{endpoint}/",
            [{"json": {"result": target}}, {"json": {"result": []}}],
        )

    with pytest.raises(
        ValueError,
        match="Schema permission .* not found in target",
    ) as exc:
        client.import_role(
            {
                "name": "Analyst",
                "permissions": [
                    "database access on [warehouse].(id:1)",
                    f"schema access on {source_path}",
                ],
                "users": [],
                "groups": [],
            },
        )

    assert "[database].[catalog].[schema]" in str(exc.value)
    assert "Update the YAML" in str(exc.value)
    assert all(request.method == "GET" for request in requests_mock.request_history)


@pytest.mark.parametrize(
    "database, catalog, schema, table, expected",
    [
        (
            "db",
            "cat",
            "public",
            "orders",
            "dataset access on [db].[cat].[public].[orders]",
        ),
        ("db", None, "public", "orders", "dataset access on [db].[].[public].[orders]"),
        ("db", None, None, "orders", "dataset access on [db].[].[].[orders]"),
        (
            "my.db",
            "my.cat",
            "my.schema",
            "my.table",
            "dataset access on [my.db].[my.cat].[my.schema].[my.table]",
        ),
        (
            "db]x",
            None,
            "public",
            "a].[b",
            "dataset access on [db]]x].[].[public].[a]].[b]",
        ),
    ],
)
def test_dataset_paths_preserve_identifier_boundaries(
    database: str,
    catalog: str | None,
    schema: str | None,
    table: str,
    expected: str,
) -> None:
    """Dots and brackets inside names must not turn into path separators."""
    assert format_dataset_permission(database, catalog, schema, table) == expected
    assert parse_dataset_permission(expected) == (
        database,
        catalog or "",
        schema or "",
        table,
    )


@pytest.mark.parametrize(
    "permission",
    [
        "dataset access on [db].[public].[orders]",
        "dataset access on [].[cat].[public].[orders]",
        "dataset access on [db].[cat].[public].[]",
    ],
)
def test_invalid_dataset_paths_are_rejected(permission: str) -> None:
    """An incomplete qualified reference must not fall back to name matching."""
    with pytest.raises(ValueError, match="Invalid qualified dataset permission"):
        parse_dataset_permission(permission)


def _datasets(schemas: list[str], catalog: str | None = "cat") -> list[dict]:
    return [
        {
            "id": index,
            "database": {"database_name": "warehouse"},
            "catalog": catalog,
            "schema": schema,
            "table_name": "orders",
        }
        for index, schema in enumerate(schemas, 11)
    ]


def _metadata(requests_mock: Mocker, base: str, datasets: list[dict]) -> None:
    requests_mock.get(
        base + "api/v1/dataset/",
        [
            {"json": {"result": datasets}},
            {"json": {"result": []}},
        ],
    )


@pytest.mark.parametrize("backend", ["superset", "preset", "legacy"])
@pytest.mark.parametrize("target_backend", ["superset", "preset", "legacy"])
def test_exported_roles_round_trip_across_backends(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
    target_backend: str,
) -> None:
    """Same-named datasets in different schemas get distinct exported paths."""
    base = (
        "https://abc.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    client = SupersetClient(base, Auth())
    mocker.patch.object(
        client,
        "get_users",
        return_value=[{"id": 21, "email": "analyst@example.com"}],
    )
    group_names = ["Analysts"] if "legacy" not in (backend, target_backend) else []
    raw = [
        {
            "id": index,
            "permission": "datasource_access",
            "view_menu": f"[warehouse].[orders](id:{dataset_id})",
        }
        for index, dataset_id in ((101, 11), (102, 12))
    ]
    if backend == "preset":
        requests_mock.get(
            base + "api/v1/security/dar/",
            [
                {
                    "json": {
                        "result": [
                            {
                                "name": "Analyst",
                                "permissions": raw,
                                "users": [{"id": 21}],
                                "groups": [{"name": name} for name in group_names],
                            },
                        ],
                    },
                },
                {"json": {"result": []}},
            ],
        )
    elif backend == "superset":
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            [
                {
                    "json": {
                        "result": [
                            {
                                "id": p["id"],
                                "permission": {"name": p["permission"]},
                                "view_menu": {"name": p["view_menu"]},
                            }
                            for p in raw
                        ],
                    },
                },
                {"json": {"result": []}},
            ],
        )
        requests_mock.get(
            base + "api/v1/security/groups/",
            [
                {
                    "json": {
                        "result": (
                            [{"id": 31, "name": "Analysts"}] if group_names else []
                        ),
                    },
                },
                {"json": {"result": []}},
            ],
        )
        requests_mock.get(
            base + "api/v1/security/roles/search/",
            [
                {
                    "json": {
                        "result": [
                            {
                                "name": "Analyst",
                                "permission_ids": [101, 102],
                                "user_ids": [21],
                                "group_ids": [31] if group_names else [],
                            },
                        ],
                    },
                },
                {"json": {"result": []}},
            ],
        )
    else:
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            status_code=404,
        )
        requests_mock.get(
            base + "roles/list/",
            [
                {
                    "text": "<table></table><table><tr><th>Roles</th></tr>"
                    '<tr><td><a href="/roles/edit/7">Edit</a></td></tr></table>',
                },
                {"text": "<table></table><table><tr><th>Roles</th></tr></table>"},
            ],
        )
        options = "".join(
            f'<option selected value="{p["id"]}">datasource access on {p["view_menu"]}</option>'
            for p in raw
        )
        requests_mock.get(
            base + "roles/edit/7",
            text='<input name="name" value="Analyst"/>'
            f'<select id="permissions">{options}</select>'
            '<select id="user"><option selected value="21">Analyst</option></select>',
        )
    _metadata(requests_mock, base, _datasets(["public", "pii"]))

    exported = client.export_roles()
    assert exported == [
        {
            "name": "Analyst",
            "users": ["analyst@example.com"],
            "groups": group_names,
            "permissions": [
                "dataset access on [warehouse].[cat].[public].[orders]",
                "dataset access on [warehouse].[cat].[pii].[orders]",
            ],
        },
    ]

    target_base = (
        "https://target.app.preset.io/"
        if target_backend == "preset"
        else "https://target.example.org/"
    )
    target = _import_target(mocker, requests_mock, target_backend, target_base)
    # Exercise the same YAML serialization used by the CLI.
    exported = yaml.safe_load(yaml.safe_dump(exported))
    target.import_role(exported[0])
    writes = [
        request
        for request in requests_mock.request_history
        if request.url.startswith(target_base) and request.method != "GET"
    ]
    if target_backend == "preset":
        assert writes[-1].json() == {
            "name": "Analyst",
            "permissions": [502, 501],
            "users": [77],
            "groups": [88] if group_names else [],
        }
    elif target_backend == "superset":
        assert [request.json() for request in writes] == [
            {"name": "Analyst"},
            {"permission_view_menu_ids": [502, 501]},
            {"user_ids": [77]},
            {"group_ids": [88] if group_names else []},
        ]
    else:
        assert writes[-1].text == "name=Analyst&user=77&permissions=502&permissions=501"


def _import_target(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
    base: str,
) -> SupersetClient:
    """Expose same-named target datasets with swapped schema identities."""
    client = SupersetClient(base, Auth())
    mocker.patch.object(
        client,
        "get_users",
        return_value=[{"id": 77, "email": "analyst@example.com"}],
    )
    target = [
        {
            "id": index,
            "permission": "datasource_access",
            "view_menu": f"[warehouse].[orders](id:{dataset_id})",
        }
        for index, dataset_id in ((501, 11), (502, 12))
    ]
    if backend == "preset":
        requests_mock.get(base + "api/v1/security/dar/", json={"result": []})
        requests_mock.get(
            base + "api/v1/security/dar_permissions/",
            [
                {"json": {"result": target}},
                {"json": {"result": []}},
            ],
        )
        requests_mock.get(
            base + "api/v1/security/groups/",
            [
                {"json": {"result": [{"id": 88, "name": "Analysts"}]}},
                {"json": {"result": []}},
            ],
        )
        requests_mock.post(base + "api/v1/security/dar/", json={"id": 42})
    elif backend == "superset":
        requests_mock.get(base + "api/v1/security/roles/search/", json={"result": []})
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            [
                {
                    "json": {
                        "result": [
                            {
                                "id": p["id"],
                                "permission": {"name": p["permission"]},
                                "view_menu": {"name": p["view_menu"]},
                            }
                            for p in target
                        ],
                    },
                },
                {"json": {"result": []}},
            ],
        )
        requests_mock.get(
            base + "api/v1/security/groups/",
            [
                {"json": {"result": [{"id": 88, "name": "Analysts"}]}},
                {"json": {"result": []}},
            ],
        )
        requests_mock.post(base + "api/v1/security/roles/", json={"id": 42})
        requests_mock.post(base + "api/v1/security/roles/42/permissions", json={})
        requests_mock.put(base + "api/v1/security/roles/42/users", json={})
        requests_mock.put(base + "api/v1/security/roles/42/groups", json={})
    else:
        requests_mock.get(
            base + "api/v1/security/permissions-resources/",
            status_code=404,
        )
        options = "".join(
            f'<option value="{p["id"]}">datasource access on {p["view_menu"]}</option>'
            for p in target
        )
        requests_mock.get(
            base + "roles/add",
            text=f'<select id="permissions">{options}</select>',
        )
        requests_mock.get(base + "roles/list/", text="<table></table>")
        requests_mock.post(base + "roles/add")
    _metadata(requests_mock, base, _datasets(["pii", "public"]))

    return client


@pytest.mark.parametrize("backend", ["superset", "preset", "legacy"])
def test_imports_resolve_the_correct_schema(
    mocker: MockerFixture,
    requests_mock: Mocker,
    backend: str,
) -> None:
    """Resolve a public grant past a same-named PII dataset with different IDs."""
    base = (
        "https://abc.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    client = _import_target(mocker, requests_mock, backend, base)

    client.import_role(
        {
            "name": "Analyst",
            "permissions": [
                "dataset access on [warehouse].[cat].[public].[orders]",
                "dataset access on [warehouse].[cat].[public].[orders]",
            ],
            "users": [],
            "groups": [],
        },
    )

    if backend == "preset":
        assert requests_mock.last_request.json()["permissions"] == [502]
    elif backend == "superset":
        request = next(
            r
            for r in requests_mock.request_history
            if r.method == "POST" and r.url.endswith("/permissions")
        )
        assert request.json() == {"permission_view_menu_ids": [502]}
    else:
        assert requests_mock.last_request.text == "name=Analyst&permissions=502"


@pytest.mark.parametrize("catalog, schema", [("cat", "pii"), ("other", "public")])
def test_unique_wrong_dataset_is_not_granted(
    requests_mock: Mocker,
    catalog: str,
    schema: str,
) -> None:
    """A unique table-name match is insufficient when its full path differs."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    _metadata(requests_mock, base, _datasets([schema], catalog))
    with pytest.raises(ValueError, match="not found in target"):
        client._resolve_role_permission_ids(
            ["dataset access on [warehouse].[cat].[public].[orders]"],
            [("datasource access on [warehouse].[orders](id:11)", 501)],
        )


def test_duplicate_full_paths_remain_ambiguous(requests_mock: Mocker) -> None:
    """Two distinct datasets with the same full path still cannot be guessed."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    _metadata(requests_mock, base, _datasets(["public", "public"]))
    with pytest.raises(ValueError, match="ambiguous in target"):
        client._resolve_role_permission_ids(
            ["dataset access on [warehouse].[cat].[public].[orders]"],
            [
                (f"datasource access on [warehouse].[orders](id:{i})", i)
                for i in (11, 12)
            ],
        )


def test_target_permission_disagreeing_with_metadata_is_rejected(
    requests_mock: Mocker,
) -> None:
    """A stale permission menu must not be rebound to a renamed dataset."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    datasets = _datasets(["public"])
    datasets[0]["table_name"] = "renamed_orders"
    _metadata(requests_mock, base, datasets)
    with pytest.raises(ValueError, match="Cannot determine full dataset path"):
        client._resolve_role_permission_ids(
            ["dataset access on [warehouse].[cat].[public].[orders]"],
            [("datasource access on [warehouse].[orders](id:11)", 501)],
        )


@pytest.mark.parametrize("unavailable", [False, True])
def test_qualified_import_requires_target_metadata(
    requests_mock: Mocker,
    unavailable: bool,
) -> None:
    """Never fall back to unsafe table-name matching for qualified input."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    requests_mock.get(
        base + "api/v1/dataset/",
        status_code=404 if unavailable else 200,
        json={"result": []},
    )
    with pytest.raises(
        ValueError,
        match="metadata is unavailable|Cannot determine full dataset path",
    ):
        client._resolve_role_permission_ids(
            ["dataset access on [warehouse].[cat].[public].[orders]"],
            [("datasource access on [warehouse].[orders](id:11)", 501)],
        )


def test_legacy_dataset_input_warns_about_missing_identity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Existing files remain readable but their matching limitation is visible."""
    assert resolve_role_permissions(
        ["datasource access on [db].[orders](id:1)"],
        [("datasource access on [db].[orders](id:99)", 50)],
    ) == [50]
    assert "matched by database and table name only" in caplog.text


@pytest.mark.parametrize(
    "missing_field",
    [
        "id",
        "database",
        "database.database_name",
        "table_name",
        "schema",
        "null_database",
    ],
)
def test_incomplete_dataset_metadata_fails_clearly(
    requests_mock: Mocker,
    missing_field: str,
) -> None:
    """Missing contract fields must fail instead of fetching details or skipping."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    dataset = _datasets(["public"])[0]
    if missing_field == "database.database_name":
        del dataset["database"]["database_name"]
    elif missing_field == "null_database":
        dataset["database"] = None
    else:
        del dataset[missing_field]
    _metadata(requests_mock, base, [dataset])

    with pytest.raises(ValueError, match="Incomplete dataset metadata"):
        client._resolve_role_permission_ids(
            ["dataset access on [warehouse].[cat].[public].[orders]"],
            [("datasource access on [warehouse].[orders](id:11)", 501)],
        )
    assert all(
        "/api/v1/dataset/?" in request.url for request in requests_mock.request_history
    )


@pytest.mark.parametrize("schema", ["public", None])
def test_dataset_schema_can_be_null_and_catalog_can_be_absent(
    requests_mock: Mocker,
    schema: str | None,
) -> None:
    """A null schema and pre-catalog OSS metadata are valid contract values."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    dataset = _datasets(["public"])[0]
    dataset["schema"] = schema
    del dataset["catalog"]
    _metadata(requests_mock, base, [dataset])

    assert client._get_dataset_permission_names({11}) == {
        11: (
            "datasource access on [warehouse].[orders]",
            f"dataset access on [warehouse].[].[{schema or ''}].[orders]",
        ),
    }


def test_missing_source_metadata_preserves_legacy_grant(
    requests_mock: Mocker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Stale or invisible dataset grants stay explicit instead of being dropped."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    _metadata(requests_mock, base, [])
    roles: list[RoleType] = [
        {
            "name": "Analyst",
            "permissions": ["datasource access on [warehouse].[orders](id:11)"],
            "users": [],
            "groups": [],
        },
    ]
    assert client._qualify_role_datasets(roles) == roles
    assert roles[0]["permissions"] == [
        "datasource access on [warehouse].[orders](id:11)",
    ]
    assert "Cannot determine full dataset path" in caplog.text


def test_dataset_metadata_requests_are_batched(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """Large exports must split dataset ID filters into bounded requests."""
    base = "https://superset.example.org/"
    client = SupersetClient(base, Auth())
    mocker.patch("preset_cli.api.clients.superset.MAX_PAGE_SIZE", 1)
    datasets = _datasets(["public", "pii"])
    requests_mock.get(
        base + "api/v1/dataset/",
        [
            {"json": {"result": [datasets[0]]}},
            {"json": {"result": []}},
            {"json": {"result": [datasets[1]]}},
            {"json": {"result": []}},
        ],
    )
    assert set(client._get_dataset_permission_names({11, 12})) == {11, 12}
    assert "value:!(11)" in requests_mock.request_history[0].qs["q"][0]
    assert "value:!(12)" in requests_mock.request_history[2].qs["q"][0]
