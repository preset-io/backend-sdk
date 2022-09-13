"""
Tests for ``preset_cli.api.clients.preset``.
"""

from requests_mock.mocker import Mocker

from preset_cli.api.clients.preset import PresetClient
from preset_cli.auth.main import Auth


def test_preset_client_get_teams(requests_mock: Mocker) -> None:
    """
    Test the ``get_teams`` method.
    """
    auth = Auth()
    requests_mock.get("https://ws.preset.io/api/v1/teams/", json={"payload": [1, 2, 3]})

    client = PresetClient("https://ws.preset.io/", auth)
    teams = client.get_teams()
    assert teams == [1, 2, 3]


def test_preset_client_get_workspaces(requests_mock: Mocker) -> None:
    """
    Test the ``get_workspaces`` method.
    """
    auth = Auth()
    requests_mock.get(
        "https://ws.preset.io/api/v1/teams/botafogo/workspaces/",
        json={"payload": [1, 2, 3]},
    )

    client = PresetClient("https://ws.preset.io/", auth)
    teams = client.get_workspaces("botafogo")
    assert teams == [1, 2, 3]


def test_preset_client_invite_users(requests_mock: Mocker) -> None:
    """
    Test the ``invite_users`` method.
    """
    auth = Auth()
    mock1 = requests_mock.post(
        "https://ws.preset.io/api/v1/teams/team1/invites/many",
    )
    mock2 = requests_mock.post(
        "https://ws.preset.io/api/v1/teams/team2/invites/many",
    )

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
