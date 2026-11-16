"""RLS imports and role lookups against targets exposing REST APIs."""

import json
from itertools import count
from typing import Any, Dict, List, cast
from urllib.parse import unquote

import pytest
from requests_mock.mocker import Mocker

from preset_cli.api.clients.superset import RuleType, SupersetClient
from preset_cli.auth.main import Auth
from preset_cli.exceptions import SupersetError

# Preset and recent Superset versions serve the roles list as a React page, so
# the legacy CRUD table is never present.
SPA_PAGE = "<!DOCTYPE html><html><body><div id='app'></div></body></html>"


def _server(
    requests_mock: Mocker,
    backend: str,
    roles: Dict[str, int],
) -> Dict[int, Dict[str, Any]]:
    """Serve RLS rules, roles and datasets, applying RLS writes to the state."""
    base = (
        "https://abc123.app.preset.io/"
        if backend == "preset"
        else "https://superset.example.org/"
    )
    role_names = {role_id: name for name, role_id in roles.items()}
    datasets = {("main", "test_table"): 1, ("main", "other_table"): 2}
    rules: Dict[int, Dict[str, Any]] = {
        7: {
            "name": "Rule name",
            "description": "Rule description",
            "filter_type": "Regular",
            "tables": [1],
            "roles": list(roles.values())[:2],
            "group_key": "department",
            "clause": "client_id = 9",
        },
    }

    def paginate(request, rows: List[Any]) -> Dict[str, Any]:
        query = unquote(request.url)
        return {"result": rows if "q=" not in query or "page:0," in query else []}

    def list_rules(request, _context) -> Dict[str, Any]:
        return paginate(
            request,
            [
                {
                    **rule,
                    "id": rule_id,
                    "tables": [
                        {"id": table_id, "schema": schema, "table_name": name}
                        for (schema, name), table_id in datasets.items()
                        if table_id in rule["tables"]
                    ],
                    "roles": [
                        {"id": role_id, "name": role_names[role_id]}
                        for role_id in rule["roles"]
                    ],
                }
                for rule_id, rule in rules.items()
            ],
        )

    def list_datasets(request, _context) -> Dict[str, Any]:
        query = unquote(request.url)
        return paginate(
            request,
            [
                {"id": table_id}
                for (schema, name), table_id in datasets.items()
                if f"value:{schema})" in query and f"value:{name})" in query
            ],
        )

    rule_ids = count(8)

    def create_rule(request, _context) -> Dict[str, Any]:
        rule_id = next(rule_ids)
        rules[rule_id] = request.json()
        return {"id": rule_id, "result": request.json()}

    def update_rule(request, _context) -> Dict[str, Any]:
        rule_id = int(request.url.rsplit("/", 1)[1])
        rules[rule_id] = {**rules[rule_id], **request.json()}
        return {"id": rule_id, "result": request.json()}

    requests_mock.get(base + "api/v1/rowlevelsecurity/", json=list_rules)
    requests_mock.post(base + "api/v1/rowlevelsecurity/", json=create_rule)
    for rule_id in range(7, 12):
        requests_mock.put(base + f"api/v1/rowlevelsecurity/{rule_id}", json=update_rule)
    requests_mock.get(base + "api/v1/dataset/", json=list_datasets)
    requests_mock.get(base + "roles/list/", text=SPA_PAGE)
    role_endpoint = "dar" if backend == "preset" else "roles/search"
    requests_mock.get(
        base + f"api/v1/security/{role_endpoint}/",
        json=lambda request, _context: paginate(
            request,
            [
                {"id": role_id, "name": name, "permissions": []}
                for name, role_id in roles.items()
            ],
        ),
    )
    return rules


def _client(backend: str) -> SupersetClient:
    return SupersetClient(
        (
            "https://abc123.app.preset.io/"
            if backend == "preset"
            else "https://superset.example.org/"
        ),
        Auth(),
    )


def _rls_writes(requests_mock: Mocker) -> List[Any]:
    return [
        (request.method, request.path, request.json())
        for request in requests_mock.request_history
        if request.method in {"POST", "PUT"}
    ]


@pytest.mark.parametrize("backend", ["preset", "superset"])
def test_import_rls_round_trips_unmodified_export(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """
    Importing an unmodified export updates each rule in place.

    On Preset the rule references Data Access Roles, which only the DAR API
    lists; ``my_dar_extra`` checks the name match is exact.
    """
    roles = {"my_dar": 11, "rls_only": 12, "my_dar_extra": 13}
    rules = _server(requests_mock, backend, roles)
    before = json.loads(json.dumps(rules))
    client = _client(backend)

    exported = list(client.export_rls())
    assert exported == [
        {
            "name": "Rule name",
            "description": "Rule description",
            "filter_type": "Regular",
            "tables": ["main.test_table"],
            "roles": ["my_dar", "rls_only"],
            "group_key": "department",
            "clause": "client_id = 9",
        },
    ]

    data = client.get_rls_import_data()
    for rule in exported:
        client.import_rls(rule, data)

    assert _rls_writes(requests_mock) == [
        (
            "PUT",
            "/api/v1/rowlevelsecurity/7",
            {
                "name": "Rule name",
                "description": "Rule description",
                "filter_type": "Regular",
                "tables": [1],
                "roles": [11, 12],
                "group_key": "department",
                "clause": "client_id = 9",
            },
        ),
    ]
    assert json.loads(json.dumps(rules)) == before
    assert list(client.export_rls()) == exported


@pytest.mark.parametrize("backend", ["preset", "superset"])
def test_import_rls_applies_role_changes(
    requests_mock: Mocker,
    backend: str,
) -> None:
    """
    Role and table changes replace the existing rule's lists, and new rules are
    created, as a fresh export shows.
    """
    roles = {"my_dar": 11, "rls_only": 12, "my_dar_extra": 13}
    _server(requests_mock, backend, roles)
    client = _client(backend)

    exported = list(client.export_rls())
    changed: List[RuleType] = [
        {**exported[0], "roles": ["my_dar_extra", "my_dar"]},
        {
            "name": "New rule",
            "description": None,
            "filter_type": "Base",
            "tables": ["main.other_table"],
            "roles": ["rls_only"],
            "group_key": None,  # type: ignore
            "clause": "1 = 0",
        },
    ]
    data = client.get_rls_import_data()
    for rule in changed:
        client.import_rls(rule, data)

    assert [(method, path) for method, path, _ in _rls_writes(requests_mock)] == [
        ("PUT", "/api/v1/rowlevelsecurity/7"),
        ("POST", "/api/v1/rowlevelsecurity/"),
    ]
    assert list(client.export_rls()) == [
        {**exported[0], "roles": ["my_dar_extra", "my_dar"]},
        {**changed[1], "roles": ["rls_only"]},
    ]

    # Removing every role is applied too, and reimporting is idempotent.
    client.import_rls({**changed[0], "roles": []}, data)
    client.import_rls(changed[1], data)
    assert list(client.export_rls())[0]["roles"] == []
    assert _rls_writes(requests_mock)[-1][0:2] == (
        "PUT",
        "/api/v1/rowlevelsecurity/8",
    )


@pytest.mark.parametrize("backend", ["preset", "superset"])
def test_import_rls_unknown_role(requests_mock: Mocker, backend: str) -> None:
    """
    A role missing from the target fails before any RLS write.
    """
    _server(requests_mock, backend, {"my_dar": 11, "rls_only": 12})
    client = _client(backend)
    rule = cast(
        RuleType,
        {**list(client.export_rls())[0], "roles": ["my_dar", "my_da"]},
    )

    with pytest.raises(Exception) as excinfo:
        client.import_rls(rule)
    assert str(excinfo.value) == "Cannot find role: my_da"
    assert _rls_writes(requests_mock) == []


def test_import_rls_preset_data_access_role(requests_mock: Mocker) -> None:
    """
    An unmodified export referencing Data Access Roles imports on Preset.

    Preset serves no roles CRUD table, so the legacy lookup failed with
    ``Cannot find role``. ``import_rls`` fetches its own lookups here.
    """
    _server(requests_mock, "preset", {"my_dar": 11, "rls_only": 12})
    client = _client("preset")
    client.import_rls(list(client.export_rls())[0])

    assert [(method, path) for method, path, _ in _rls_writes(requests_mock)] == [
        ("PUT", "/api/v1/rowlevelsecurity/7"),
    ]


def test_get_rls_import_data_error(requests_mock: Mocker) -> None:
    """
    Errors other than a missing RLS API are raised.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/rowlevelsecurity/",
        status_code=500,
        json={"message": "boom"},
        headers={"content-type": "application/json"},
    )
    with pytest.raises(SupersetError):
        _client("superset").get_rls_import_data()


@pytest.mark.parametrize("backend", ["preset", "superset"])
def test_get_role_id(requests_mock: Mocker, backend: str) -> None:
    """
    ``get_role_id`` resolves exact names through the role API of the target.
    """
    _server(requests_mock, backend, {"my_dar": 11, "my_dar_extra": 13})
    client = _client(backend)

    assert client.get_role_id("my_dar") == 11
    assert client.get_role_id("my_dar_extra") == 13
    with pytest.raises(Exception) as excinfo:
        client.get_role_id("my_da")
    assert str(excinfo.value) == "Cannot find role: my_da"


def _legacy_roles(requests_mock: Mocker, status_code: int) -> None:
    """Serve a target without the roles search API and a legacy roles table."""
    base = "https://superset.example.org/"
    requests_mock.get(
        base + "api/v1/security/roles/search/",
        status_code=status_code,
        json={"message": "error"},
        headers={"content-type": "application/json"},
    )
    requests_mock.get(
        base + "roles/list/?_flt_3_name=Gamma",
        text="""
<table></table>
<table>
  <tr><td></td><td>Name</td></tr>
  <tr><td><input id="4" /></td><td>Gamma</td></tr>
</table>
        """,
    )


def test_get_role_id_legacy_fallback(requests_mock: Mocker) -> None:
    """
    Targets without the roles search API use the legacy roles table.
    """
    _legacy_roles(requests_mock, 404)
    assert _client("superset").get_role_id("Gamma") == 4


def test_get_role_id_error(requests_mock: Mocker) -> None:
    """
    Errors other than a missing roles search API are raised.
    """
    _legacy_roles(requests_mock, 500)
    with pytest.raises(SupersetError):
        _client("superset").get_role_id("Gamma")


def test_import_rls_legacy_role_lookup(requests_mock: Mocker) -> None:
    """
    Targets with the RLS API but no roles search API resolve roles from the
    legacy roles table and still write through the RLS API.
    """
    _server(requests_mock, "superset", {"Gamma": 4})
    _legacy_roles(requests_mock, 404)
    client = _client("superset")
    data = client.get_rls_import_data()
    assert data["roles"] is None

    client.import_rls({**list(client.export_rls())[0], "roles": ["Gamma"]}, data)

    assert _rls_writes(requests_mock)[0][0:2] == (
        "PUT",
        "/api/v1/rowlevelsecurity/7",
    )
    assert _rls_writes(requests_mock)[0][2]["roles"] == [4]


def test_update_role_preset(requests_mock: Mocker) -> None:
    """
    ``update_role`` replaces only the given relationships of a Preset DAR.
    """
    _server(requests_mock, "preset", {"my_dar": 11, "rls_only": 12})
    put = requests_mock.put(
        "https://abc123.app.preset.io/api/v1/security/dar/11",
        json={},
    )
    client = _client("preset")

    client.update_role(client.get_role_id("my_dar"), user=[1, 2])
    client.update_role(11, name="renamed", permissions=[5])

    assert [request.json() for request in put.request_history] == [
        {"name": "my_dar", "users": [1, 2]},
        {"name": "renamed", "permissions": [5]},
    ]
    with pytest.raises(Exception) as excinfo:
        client.update_role(99, user=[1])
    assert str(excinfo.value) == "Cannot find role: 99"
