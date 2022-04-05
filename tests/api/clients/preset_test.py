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
