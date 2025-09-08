"""
Tests for preset_cli.cli.export_users
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock

import yaml
from click.testing import CliRunner
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.export_users import export_users


def test_export_users_no_teams(mocker: MockerFixture) -> None:
    """
    Test export_users when no teams are found.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_client.get_teams.return_value = []

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

    assert result.exit_code == 0
    assert "No teams found." in result.output


def test_export_users_with_team_filter(mocker: MockerFixture) -> None:
    """
    Test export_users with team filtering.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
        {"name": "team2", "title": "Team Two"},
        {"name": "team3", "title": "Team Three"},
    ]
    mock_client.get_team_members.return_value = []
    mock_client.get_workspaces.return_value = []

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["--teams", "team1", "--teams", "Team Three", "output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        assert "Processing team: Team One" in result.output
        assert "Processing team: Team Three" in result.output
        assert "Team Two" not in result.output

        # Verify the output file was created
        assert Path("output.yaml").exists()
        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            assert data == []  # No users since we mocked empty responses


def test_export_users_full_flow(mocker: MockerFixture) -> None:
    """
    Test export_users with complete data flow.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_client.session = mock_session

    mock_client.get_base_url.return_value = URL("https://api.preset.io/v1")

    # Mock teams
    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    # Mock team members
    mock_client.get_team_members.return_value = [
        {
            "user": {
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Smith",
                "username": "alice",
            },
            "team_role_id": 1,  # Admin
        },
        {
            "user": {
                "email": "bob@example.com",
                "first_name": "Bob",
                "last_name": "Jones",
                "username": "bob",
            },
            "team_role_id": 2,  # User
        },
    ]

    # Mock workspaces
    mock_client.get_workspaces.return_value = [
        {"id": 1, "title": "Workspace One", "name": "ws1"},
        {"id": 2, "title": "Workspace Two", "name": "ws2"},
    ]

    # Mock workspace memberships
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    # First workspace memberships
    ws1_response = MagicMock()
    ws1_response.raise_for_status.return_value = None
    ws1_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": "alice@example.com",
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                },
                "workspace_role": {
                    "role_identifier": "Admin",
                    "name": "Workspace Admin",
                },
            },
            {
                "user": {
                    "email": "bob@example.com",
                    "first_name": "Bob",
                    "last_name": "Jones",
                    "username": "bob",
                },
                "workspace_role": {"role_identifier": "PresetAlpha", "name": "Alpha"},
            },
        ],
        "meta": {"count": 2},
    }

    # Second workspace memberships
    ws2_response = MagicMock()
    ws2_response.raise_for_status.return_value = None
    ws2_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": "alice@example.com",
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                },
                "workspace_role": {"role_identifier": "PresetBeta", "name": "Beta"},
            },
        ],
        "meta": {"count": 1},
    }

    # Configure mock session.get to return different responses based on URL
    def side_effect(url):
        if "workspaces/1/memberships" in str(url):
            return ws1_response
        if "workspaces/2/memberships" in str(url):
            return ws2_response
        return mock_response

    mock_session.get.side_effect = side_effect

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        assert "Processing team: Team One" in result.output
        assert "Processing workspace: Workspace One" in result.output
        assert "Processing workspace: Workspace Two" in result.output
        assert "Exported 2 users to" in result.output
        assert "output.yaml" in result.output

        # Verify the output file content
        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        assert len(data) == 2

        # Check Alice's data
        alice = next(u for u in data if u["email"] == "alice@example.com")
        assert alice["first_name"] == "Alice"
        assert alice["last_name"] == "Smith"
        assert alice["username"] == "alice"
        assert alice["teams"]["Team One"] == "admin"
        assert (
            alice["workspaces"]["Team One/Workspace One"]["workspace_role"]
            == "workspace admin"
        )
        assert (
            alice["workspaces"]["Team One/Workspace Two"]["workspace_role"]
            == "secondary contributor"
        )

        # Check Bob's data
        bob = next(u for u in data if u["email"] == "bob@example.com")
        assert bob["first_name"] == "Bob"
        assert bob["last_name"] == "Jones"
        assert bob["username"] == "bob"
        assert bob["teams"]["Team One"] == "user"
        assert (
            bob["workspaces"]["Team One/Workspace One"]["workspace_role"]
            == "primary contributor"
        )
        assert "Team One/Workspace Two" not in bob["workspaces"]


def test_export_users_with_pagination(mocker: MockerFixture) -> None:
    """
    Test export_users with pagination in workspace memberships.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_client.session = mock_session

    mock_client.get_base_url.return_value = URL("https://api.preset.io/v1")

    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    mock_client.get_team_members.return_value = []
    mock_client.get_workspaces.return_value = [
        {"id": 1, "title": "Workspace One", "name": "ws1"},
    ]

    # Mock paginated responses
    page1_response = MagicMock()
    page1_response.raise_for_status.return_value = None
    page1_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": f"user{i}@example.com",
                    "first_name": f"User{i}",
                    "last_name": "Test",
                    "username": f"user{i}",
                },
                "workspace_role": {"role_identifier": "PresetAlpha", "name": "Alpha"},
            }
            for i in range(250)
        ],
        "meta": {"count": 300},  # More than 250, so pagination needed
    }

    page2_response = MagicMock()
    page2_response.raise_for_status.return_value = None
    page2_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": f"user{i}@example.com",
                    "first_name": f"User{i}",
                    "last_name": "Test",
                    "username": f"user{i}",
                },
                "workspace_role": {"role_identifier": "PresetAlpha", "name": "Alpha"},
            }
            for i in range(250, 300)
        ],
        "meta": {"count": 300},
    }

    # Configure mock to return different pages
    call_count = 0

    def side_effect(url):
        nonlocal call_count
        if "page_number=1" in str(url):
            return page1_response
        if "page_number=2" in str(url):
            return page2_response
        return MagicMock()

    mock_session.get.side_effect = side_effect

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        assert "Exported 300 users to" in result.output
        assert "output.yaml" in result.output

        # Verify we got all users
        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        assert len(data) == 300


def test_export_users_error_handling(mocker: MockerFixture) -> None:
    """
    Test export_users error handling for failed API calls.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_client.session = mock_session

    mock_client.get_base_url.return_value = URL("https://api.preset.io/v1")

    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    # Mock team members to fail
    mock_client.get_team_members.side_effect = Exception("API Error")

    # Mock workspaces to succeed
    mock_client.get_workspaces.return_value = [
        {"id": 1, "title": "Workspace One", "name": "ws1"},
    ]

    # Mock workspace membership to fail
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("Connection Error")
    mock_session.get.return_value = mock_response

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        assert "Warning: Failed to get team members: API Error" in result.output
        assert (
            "Warning: Failed to get workspace memberships: Connection Error"
            in result.output
        )

        # Should still create empty file
        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        assert data == []


def test_export_users_workspace_error(mocker: MockerFixture) -> None:
    """
    Test export_users when getting workspaces fails.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    mock_client.get_team_members.return_value = [
        {
            "user": {
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Smith",
                "username": "alice",
            },
            "team_role_id": 1,
        },
    ]

    # Mock workspaces to fail
    mock_client.get_workspaces.side_effect = Exception("Workspace API Error")

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        assert "Warning: Failed to get workspaces: Workspace API Error" in result.output

        # Should still export team-level data
        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        assert len(data) == 1
        assert data[0]["email"] == "alice@example.com"
        assert data[0]["teams"]["Team One"] == "admin"


def test_export_users_default_filename(mocker: MockerFixture) -> None:
    """
    Test export_users with default filename.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_client.get_teams.return_value = []

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            [],  # No filename provided, should use default
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0
        # The default filename from the command definition
        assert "No teams found." in result.output


def test_export_users_case_sensitive_emails(mocker: MockerFixture) -> None:
    """
    Test that emails are handled case-sensitively.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_client.session = mock_session

    mock_client.get_base_url.return_value = URL("https://api.preset.io/v1")

    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    # Team members with mixed case emails
    mock_client.get_team_members.return_value = [
        {
            "user": {
                "email": "Alice@Example.COM",
                "first_name": "Alice",
                "last_name": "Smith",
                "username": "alice",
            },
            "team_role_id": 1,
        },
    ]

    mock_client.get_workspaces.return_value = [
        {"id": 1, "title": "Workspace One", "name": "ws1"},
    ]

    # Workspace membership with same case email
    ws_response = MagicMock()
    ws_response.raise_for_status.return_value = None
    ws_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": "Alice@Example.COM",  # same case as team member
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                },
                "workspace_role": {
                    "role_identifier": "Admin",
                    "name": "Workspace Admin",
                },
            },
        ],
        "meta": {"count": 1},
    }

    mock_session.get.return_value = ws_response

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0

        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        # Should have only one user (emails consolidated)
        assert len(data) == 1
        assert data[0]["email"] == "Alice@Example.COM"  # Preserves original case


def test_export_users_no_access_filtered_out(mocker: MockerFixture) -> None:
    """
    Test that workspaces with "PresetNoAccess" role are filtered out.
    """
    runner = CliRunner()

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_client.session = mock_session

    mock_client.get_base_url.return_value = URL("https://api.preset.io/v1")

    mock_client.get_teams.return_value = [
        {"name": "team1", "title": "Team One"},
    ]

    # Team members
    mock_client.get_team_members.return_value = [
        {
            "user": {
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Smith",
                "username": "alice",
            },
            "team_role_id": 1,  # Admin
        },
    ]

    mock_client.get_workspaces.return_value = [
        {"id": 1, "title": "Workspace One", "name": "ws1"},
        {"id": 2, "title": "Workspace Two", "name": "ws2"},
    ]

    # Mock workspace memberships - one with access, one with no access
    ws1_response = MagicMock()
    ws1_response.raise_for_status.return_value = None
    ws1_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": "alice@example.com",
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                },
                "workspace_role": {
                    "role_identifier": "Admin",
                    "name": "Workspace Admin",
                },  # Has access
            },
        ],
        "meta": {"count": 1},
    }

    # Second workspace - no access role
    ws2_response = MagicMock()
    ws2_response.raise_for_status.return_value = None
    ws2_response.json.return_value = {
        "payload": [
            {
                "user": {
                    "email": "alice@example.com",
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                },
                "workspace_role": {
                    "role_identifier": "PresetNoAccess",
                    "name": "No Access",
                },  # No access - should be filtered
            },
        ],
        "meta": {"count": 1},
    }

    def side_effect(url):
        if "workspaces/1/memberships" in str(url):
            return ws1_response
        if "workspaces/2/memberships" in str(url):
            return ws2_response
        return MagicMock()

    mock_session.get.side_effect = side_effect

    mocker.patch("preset_cli.cli.export_users.PresetClient", return_value=mock_client)

    with runner.isolated_filesystem():
        result = runner.invoke(
            export_users,
            ["output.yaml"],
            obj={"AUTH": Mock(), "MANAGER_URL": "https://api.preset.io/"},
        )

        assert result.exit_code == 0

        with open("output.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        assert len(data) == 1
        alice = data[0]
        assert alice["email"] == "alice@example.com"
        assert alice["teams"]["Team One"] == "admin"

        # Should only have Workspace One (has access), not Workspace Two (no access)
        assert "Team One/Workspace One" in alice["workspaces"]
        assert "Team One/Workspace Two" not in alice["workspaces"]

        # Verify the workspace we do have has the correct role
        assert (
            alice["workspaces"]["Team One/Workspace One"]["workspace_role"]
            == "workspace admin"
        )
