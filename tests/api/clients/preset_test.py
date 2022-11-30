"""
Tests for ``preset_cli.api.clients.preset``.
"""

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.api.clients.preset import PresetClient
from preset_cli.auth.main import Auth


def test_preset_client_get_teams(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test the ``get_teams`` method.
    """
    _logger = mocker.patch("preset_cli.api.clients.preset._logger")
    requests_mock.get("https://ws.preset.io/v1/teams", json={"payload": [1, 2, 3]})

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    teams = client.get_teams()
    assert teams == [1, 2, 3]
    _logger.debug.assert_called_with(
        "GET %s",
        URL("https://ws.preset.io/v1/teams"),
    )


def test_preset_client_get_workspaces(requests_mock: Mocker) -> None:
    """
    Test the ``get_workspaces`` method.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams/botafogo/workspaces",
        json={"payload": [1, 2, 3]},
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    teams = client.get_workspaces("botafogo")
    assert teams == [1, 2, 3]


def test_preset_client_invite_users(requests_mock: Mocker) -> None:
    """
    Test the ``invite_users`` method.
    """
    mock1 = requests_mock.post(
        "https://ws.preset.io/v1/teams/team1/invites/many",
    )
    mock2 = requests_mock.post(
        "https://ws.preset.io/v1/teams/team2/invites/many",
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    client.invite_users(["team1", "team2"], ["adoe@example.com", "bdoe@example.com"])

    assert (
        mock1.last_request.json()
        == mock2.last_request.json()
        == {
            "invites": [
                {"email": "adoe@example.com", "team_role_id": 2},
                {"email": "bdoe@example.com", "team_role_id": 2},
            ],
        }
    )


def test_preset_client_export_users(requests_mock: Mocker) -> None:
    """
    Test the ``export_users`` method.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams",
        json={
            "payload": [{"name": "team1"}],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/v1/teams/team1/workspaces",
        json={
            "payload": [
                {"id": 1, "hostname": "other.example.org"},
                {"id": 2, "hostname": "superset.example.org"},
                {"id": 3, "hostname": "another.example.org"},
            ],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/v1/teams/team1/workspaces/2/memberships",
        json={
            "payload": [
                {
                    "user": {
                        "username": "adoe",
                        "first_name": "Alice",
                        "last_name": "Doe",
                        "email": "adoe@example.com",
                    },
                },
                {
                    "user": {
                        "username": "bdoe",
                        "first_name": "Bob",
                        "last_name": "Doe",
                        "email": "bdoe@example.com",
                    },
                },
                {
                    "user": {
                        "username": "cdoe",
                        "first_name": "Clarisse",
                        "last_name": "Doe",
                        "email": "cdoe@example.com",
                    },
                },
            ],
        },
    )
    requests_mock.get(
        "https://superset.example.org/roles/add",
        text="""
<select id="user">
    <option value="1">Alice Doe</option>
    <option value="2">Bob Doe</option>
</select>
    """,
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    assert list(client.export_users(URL("https://superset.example.org/"))) == [
        {
            "id": 1,
            "first_name": "Alice",
            "last_name": "Doe",
            "username": "adoe",
            "email": "adoe@example.com",
            "role": [],
        },
        {
            "id": 2,
            "first_name": "Bob",
            "last_name": "Doe",
            "username": "bdoe",
            "email": "bdoe@example.com",
            "role": [],
        },
    ]


def test_preset_client_export_users_no_teams(requests_mock: Mocker) -> None:
    """
    Test the ``export_users`` method when no teams exist.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams",
        json={"payload": []},
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    with pytest.raises(Exception) as excinfo:
        list(client.export_users(URL("https://superset.example.org/")))
    assert str(excinfo.value) == "Unable to find workspace and/or team"


def test_preset_client_export_users_no_workspaces(requests_mock: Mocker) -> None:
    """
    Test the ``export_users`` method when no teams exist.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams",
        json={
            "payload": [{"name": "team1"}],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/v1/teams/team1/workspaces",
        json={"payload": []},
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    with pytest.raises(Exception) as excinfo:
        list(client.export_users(URL("https://superset.example.org/")))
    assert str(excinfo.value) == "Unable to find workspace and/or team"


def test_preset_client_import_users(requests_mock: Mocker) -> None:
    """
    Test the ``import_users`` method.
    """
    requests_mock.post("https://ws.preset.io/v1/teams/team1/scim/v2/Users")

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    client.import_users(
        ["team1"],
        [
            {
                "id": 1,
                "username": "adoe",
                "role": [],
                "first_name": "Alice",
                "last_name": "Doe",
                "email": "adoe@example.com",
            },
        ],
    )

    assert requests_mock.last_request.headers["Content-Type"] == "application/scim+json"
    assert requests_mock.last_request.headers["Accept"] == "application/scim+json"
    assert requests_mock.last_request.json() == {
        "schemas": [
            "urn:ietf:params:scim:schemas:core:2.0:User",
            "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User",
        ],
        "active": True,
        "displayName": "Alice Doe",
        "emails": [{"primary": True, "type": "work", "value": "adoe@example.com"}],
        "meta": {"resourceType": "User"},
        "userName": "adoe@example.com",
        "name": {"formatted": "Alice Doe", "familyName": "Doe", "givenName": "Alice"},
    }


def test_preset_client_import_users_idempotency(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """
    Test the ``import_users`` method when a user already exists.
    """
    _logger = mocker.patch("preset_cli.api.clients.preset._logger")
    requests_mock.post(
        "https://ws.preset.io/v1/teams/team1/scim/v2/Users",
        status_code=409,
        json={
            "detail": "User already exists in the database and team.",
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
            "status": "409",
        },
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    client.import_users(
        ["team1"],
        [
            {
                "id": 1,
                "username": "adoe",
                "role": [],
                "first_name": "Alice",
                "last_name": "Doe",
                "email": "adoe@example.com",
            },
        ],
    )

    _logger.info.assert_called_with("User already exists in the database and team.")


def test_get_team_members(requests_mock: Mocker) -> None:
    """
    Test the ``get_team_members`` method.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams/botafogo/memberships",
        json={
            "payload": [
                {
                    "user": {
                        "username": "adoe",
                        "first_name": "Alice",
                        "last_name": "Doe",
                        "email": "adoe@example.com",
                    },
                },
                {
                    "user": {
                        "username": "bdoe",
                        "first_name": "Bob",
                        "last_name": "Doe",
                        "email": "bdoe@example.com",
                    },
                },
                {
                    "user": {
                        "username": "cdoe",
                        "first_name": "Clarisse",
                        "last_name": "Doe",
                        "email": "cdoe@example.com",
                    },
                },
            ],
        },
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    assert client.get_team_members("botafogo") == [
        {
            "user": {
                "username": "adoe",
                "first_name": "Alice",
                "last_name": "Doe",
                "email": "adoe@example.com",
            },
        },
        {
            "user": {
                "username": "bdoe",
                "first_name": "Bob",
                "last_name": "Doe",
                "email": "bdoe@example.com",
            },
        },
        {
            "user": {
                "username": "cdoe",
                "first_name": "Clarisse",
                "last_name": "Doe",
                "email": "cdoe@example.com",
            },
        },
    ]


def test_change_team_role(requests_mock: Mocker) -> None:
    """
    Test the ``change_team_role`` method.
    """
    requests_mock.patch("https://ws.preset.io/v1/teams/botafogo/memberships/1")

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    client.change_team_role("botafogo", 1, 2)

    assert requests_mock.last_request.json() == {"team_role_id": 2}


def test_change_workspace_role(requests_mock: Mocker) -> None:
    """
    Test the ``change_workspace_role`` method.
    """
    requests_mock.put(
        "https://ws.preset.io/v1/teams/botafogo/workspaces/1/membership",
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    client.change_workspace_role("botafogo", 1, 2, "PresetAlpha")

    assert requests_mock.last_request.json() == {
        "role_identifier": "PresetAlpha",
        "user_id": 2,
    }


def test_get_group_membership(requests_mock: Mocker) -> None:
    """
    Test the ``get_groups`` method.
    """
    requests_mock.get(
        "https://ws.preset.io/v1/teams/testSlug/scim/v2/Groups",
        json={
            "Resources": [
                {
                    "displayName": "SCIM First Test Group",
                    "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                    "members": [
                        {
                            "display": "Test Account 01",
                            "value": "samlp|example|testaccount01@example.com",
                        },
                        {
                            "display": "Test Account 02",
                            "value": "samlp|example|testaccount02@example.com",
                        },
                    ],
                    "meta": {
                        "resourceType": "Group",
                    },
                    "schemas": [
                        "urn:ietf:params:scim:schemas:core:2.0:Group",
                    ],
                },
                {
                    "displayName": "SCIM Second Test Group",
                    "id": "fba067fc-506a-452b-8cf4-7d98f6960a6b",
                    "members": [
                        {
                            "display": "Test Account 02",
                            "value": "samlp|example|testaccount02@example.com",
                        },
                    ],
                    "meta": {
                        "resourceType": "Group",
                    },
                    "schemas": [
                        "urn:ietf:params:scim:schemas:core:2.0:Group",
                    ],
                },
            ],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 2,
        },
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    assert client.get_group_membership("testSlug", 1) == {
        "Resources": [
            {
                "displayName": "SCIM First Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                    {
                        "display": "Test Account 02",
                        "value": "samlp|example|testaccount02@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
            {
                "displayName": "SCIM Second Test Group",
                "id": "fba067fc-506a-452b-8cf4-7d98f6960a6b",
                "members": [
                    {
                        "display": "Test Account 02",
                        "value": "samlp|example|testaccount02@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 2,
    }
