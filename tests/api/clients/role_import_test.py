"""Explicitly shared lookups and repeated writes during role imports."""

from itertools import count
from urllib.parse import unquote

import pytest
from requests_mock.mocker import Mocker

from preset_cli.api.clients.superset import RoleType, SupersetClient
from preset_cli.auth.main import Auth
from preset_cli.exceptions import SupersetError


def _permission(backend: str) -> dict:
    if backend == "preset":
        return {
            "id": 51,
            "permission": "datasource_access",
            "view_menu": "[db].[orders](id:11)",
        }
    return {
        "id": 51,
        "permission": {"name": "datasource_access"},
        "view_menu": {"name": "[db].[orders](id:11)"},
    }


def _server(requests_mock: Mocker, backend: str) -> tuple[SupersetClient, dict]:
    """Serve paginated reference data and record real HTTP mutations."""
    base = (
        "https://batch.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    permission_endpoint = (
        "dar_permissions" if backend == "preset" else "permissions-resources"
    )
    role_endpoint = "dar" if backend == "preset" else "roles/search"
    rows = {
        f"security/{permission_endpoint}": [_permission(backend)],
        f"security/{role_endpoint}": [],
        "security/users": [
            {
                "id": 7,
                "email": "member@example.com",
                "first_name": "A",
                "last_name": "Member",
                "username": "member",
            },
        ],
        "security/groups": [{"id": 8, "name": "Team"}],
        "dataset": [
            {
                "id": 11,
                "database": {"database_name": "db"},
                "catalog": "cat",
                "schema": "public",
                "table_name": "orders",
            },
        ],
    }
    reads = {}
    for endpoint, data in rows.items():
        reads[endpoint] = requests_mock.get(
            base + f"api/v1/{endpoint}/",
            json=lambda request, _, data=data: {
                "result": data if "page:0," in unquote(request.url) else [],
            },
        )
    resource = "dar" if backend == "preset" else "roles"
    ids = count(42)
    reads["created"] = requests_mock.post(
        base + f"api/v1/security/{resource}/",
        json=lambda _request, _context: {"id": next(ids)},
    )
    for role_id in (42, 43, 44):
        if backend == "preset":
            requests_mock.put(base + f"api/v1/security/dar/{role_id}", json={})
        else:
            requests_mock.post(
                base + f"api/v1/security/roles/{role_id}/permissions",
                json={},
            )
            requests_mock.put(base + f"api/v1/security/roles/{role_id}/users", json={})
            requests_mock.put(base + f"api/v1/security/roles/{role_id}/groups", json={})
    return SupersetClient(base, Auth()), reads


def _role(name: str = "Analyst") -> RoleType:
    return {
        "name": name,
        "permissions": ["dataset access on [db].[cat].[public].[orders]"],
        "users": ["member@example.com"],
        "groups": ["Team"],
    }


@pytest.mark.parametrize(
    "names",
    [[], ["Gamma"], ["Gamma_Preset"], ["Gamma", "Alpha_Preset"]],
)
def test_preset_role_name_validation_accepts_non_colliding_roles(
    names: list[str],
) -> None:
    """Built-ins and suffixed custom names are valid without a conflicting pair."""
    client = SupersetClient("https://test.app.preset.io/", Auth())
    client.validate_role_import_names([_role(name) for name in names])


@pytest.mark.parametrize("backend", ["superset", "preset"])
def test_reference_lookups_are_shared_explicitly(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """Supplied lookups load once; independent calls fetch new reference data."""
    client, reads = _server(requests_mock, backend)
    data = client.get_role_import_data(_role())
    for name in ("One", "Two", "One"):
        client.import_role(_role(name), data)
    assert reads["created"].call_count == 2
    for endpoint, request in reads.items():
        if endpoint != "created":
            assert request.call_count == (
                1 if endpoint.endswith(("roles/search", "dar")) else 2
            )

    client.import_role(_role("Three"))
    for endpoint, request in reads.items():
        if endpoint != "created":
            assert request.call_count == (
                2 if endpoint.endswith(("roles/search", "dar")) else 4
            )


@pytest.mark.parametrize("backend", ["superset", "preset"])
def test_empty_memberships_do_not_list_users_or_groups(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """Clearing memberships still writes empty lists without expensive lookups."""
    client, reads = _server(requests_mock, backend)
    role = _role()
    role["users"], role["groups"] = [], []
    client.import_role(role)
    assert not reads["security/users"].called
    assert not reads["security/groups"].called
    writes = [
        request.json()
        for request in requests_mock.request_history
        if request.method != "GET"
    ]
    if backend == "preset":
        assert writes[-1] == {
            "name": "Analyst",
            "permissions": [51],
            "users": [],
            "groups": [],
        }
    else:
        assert {"user_ids": []} in writes
        assert {"group_ids": []} in writes


@pytest.mark.parametrize("backend", ["superset", "preset"])
def test_independent_imports_fetch_fresh_memberships(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """A failed run cannot leave stale reference data on a reused API client."""
    client, _ = _server(requests_mock, backend)
    role = _role()
    role["users"] = ["invited@example.com"]
    with pytest.raises(ValueError, match="User invited@example.com not found"):
        client.import_role(role)
    requests_mock.get(
        str(client.baseurl) + "api/v1/security/users/",
        [
            {
                "json": {
                    "result": [
                        {
                            "id": 9,
                            "email": "invited@example.com",
                            "first_name": "New",
                            "last_name": "Member",
                            "username": "invited",
                        },
                    ],
                },
            },
            {"json": {"result": []}},
        ],
    )
    client.import_role(role)
    writes = [
        request.json()
        for request in requests_mock.request_history
        if request.method != "GET"
    ]
    assert (
        ({"user_ids": [9]} in writes)
        if backend == "superset"
        else writes[-1]["users"] == [9]
    )


@pytest.mark.parametrize("backend", ["superset", "preset"])
def test_failed_lookup_is_retried_for_later_roles(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """Continue-on-error can recover from a transient read failure within a run."""
    client, _ = _server(requests_mock, backend)
    endpoint = "dar_permissions" if backend == "preset" else "permissions-resources"
    read = requests_mock.get(
        str(client.baseurl) + f"api/v1/security/{endpoint}/",
        [
            {"status_code": 503},
            {"json": {"result": [_permission(backend)]}},
            {"json": {"result": []}},
        ],
    )
    with pytest.raises(SupersetError):
        client.get_role_import_data(_role("Failed"))
    role = _role("Successful")
    data = client.get_role_import_data(role)
    client.import_role(role, data)
    assert read.call_count == 3


def test_created_role_is_remembered_after_relationship_write_fails(
    requests_mock: Mocker,
) -> None:
    """A retry must update the role already created before a partial failure."""
    client, reads = _server(requests_mock, "superset")
    write = requests_mock.post(
        str(client.baseurl) + "api/v1/security/roles/42/permissions",
        [{"status_code": 503}, {"json": {}}],
    )
    role = _role()
    data = client.get_role_import_data(role)
    with pytest.raises(SupersetError):
        client.import_role(role, data)
    client.import_role(role, data)
    assert reads["created"].call_count == 1
    assert write.call_count == 2


def test_dar_snapshot_tracks_replaced_permissions(
    requests_mock: Mocker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A repeated empty import warns about removed permissions only once."""
    client, reads = _server(requests_mock, "preset")
    role = _role()
    data = client.get_role_import_data(role)
    client.import_role(role, data)
    role["permissions"] = ["can read on Chart"]
    client.import_role(role, data)
    client.import_role(role, data)
    assert reads["created"].call_count == 1
    assert (
        sum(
            "overwritten as an empty Data Access Role" in text
            for text in caplog.messages
        )
        == 1
    )


def test_legacy_run_reuses_detection_and_permission_form(requests_mock: Mocker) -> None:
    """Legacy imports reuse their capability result, users, and form choices."""
    client, reads = _server(requests_mock, "superset")
    base = str(client.baseurl)
    detection = requests_mock.get(
        base + "api/v1/security/permissions-resources/",
        status_code=404,
    )
    form = requests_mock.get(
        base + "roles/add",
        text='<select id="permissions"><option value="51">'
        "datasource access on [db].[orders](id:11)</option></select>",
    )
    requests_mock.get(base + "roles/list/", text="<table></table>")
    created = requests_mock.post(base + "roles/add")
    role = _role()
    role["groups"] = []
    data = client.get_role_import_data(role)
    for name in ("One", "Two"):
        client.import_role({**role, "name": name}, data)
    assert detection.call_count == form.call_count == 1
    assert reads["security/users"].call_count == reads["dataset"].call_count == 2
    assert not reads["security/groups"].called
    assert created.call_count == 2
