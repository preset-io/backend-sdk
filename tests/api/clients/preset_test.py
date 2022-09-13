"""
Tests for ``preset_cli.api.clients.preset``.
"""

import pytest
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.api.clients.preset import PresetClient
from preset_cli.auth.main import Auth


def test_preset_client_get_teams(requests_mock: Mocker) -> None:
    """
    Test the ``get_teams`` method.
    """
    requests_mock.get("https://ws.preset.io/api/v1/teams/", json={"payload": [1, 2, 3]})

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    teams = client.get_teams()
    assert teams == [1, 2, 3]


def test_preset_client_get_workspaces(requests_mock: Mocker) -> None:
    """
    Test the ``get_workspaces`` method.
    """
    requests_mock.get(
        "https://ws.preset.io/api/v1/teams/botafogo/workspaces/",
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
        "https://ws.preset.io/api/v1/teams/team1/invites/many",
    )
    mock2 = requests_mock.post(
        "https://ws.preset.io/api/v1/teams/team2/invites/many",
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
        "https://ws.preset.io/api/v1/teams/",
        json={
            "payload": [{"name": "team1"}],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/api/v1/teams/team1/workspaces/",
        json={
            "payload": [
                {"id": 1, "hostname": "other.example.org"},
                {"id": 2, "hostname": "superset.example.org"},
                {"id": 3, "hostname": "another.example.org"},
            ],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/api/v1/teams/team1/workspaces/2/memberships",
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
        "https://ws.preset.io/api/v1/teams/",
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
        "https://ws.preset.io/api/v1/teams/",
        json={
            "payload": [{"name": "team1"}],
        },
    )
    requests_mock.get(
        "https://ws.preset.io/api/v1/teams/team1/workspaces/",
        json={"payload": []},
    )

    auth = Auth()
    client = PresetClient("https://ws.preset.io/", auth)
    with pytest.raises(Exception) as excinfo:
        list(client.export_users(URL("https://superset.example.org/")))
    assert str(excinfo.value) == "Unable to find workspace and/or team"
