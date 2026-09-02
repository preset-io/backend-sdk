"""
Test authentication mechanisms.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from preset_cli.auth.main import Auth


def test_auth(mocker: MockerFixture) -> None:
    """
    Tests for the base class ``Auth``.
    """
    # pylint: disable=invalid-name
    Session = mocker.patch("preset_cli.auth.main.Session")

    auth = Auth()
    assert auth.session == Session()
    assert auth.get_headers() == {}


def test_reauth(requests_mock: Mocker) -> None:
    """
    Test the ``reauth`` hook when authentication fails.
    """
    requests_mock.get("http://example.org/", status_code=401)

    # the base class has no reauth
    auth = Auth()
    response = auth.session.get("http://example.org/")
    assert response.status_code == 401


def test_reauth_retries_a_persistent_401_once(requests_mock: Mocker) -> None:
    """A failed reauthentication does not trigger another retry."""

    class Reauth(Auth):
        """Auth with a refresh counter for the retry-bound test."""

        def __init__(self):
            super().__init__()
            self.auth_calls = 0

        def auth(self) -> None:
            self.auth_calls += 1

        def get_headers(self) -> dict[str, str]:
            return {"Authorization": "Bearer refreshed"}

    auth = Reauth()
    requests_mock.get("http://example.org/", status_code=401)

    response = auth.session.get("http://example.org/")
    assert response.status_code == 401
    assert auth.auth_calls == 1
    assert len(requests_mock.request_history) == 2


def test_reauth_replay_redirected_401_preserves_unrelated_hooks(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """A replay's redirected 401 cannot start another reauthentication."""

    class Reauth(Auth):
        """Auth with a refresh counter for the redirect-bound test."""

        def __init__(self):
            super().__init__()
            self.auth_calls = 0

        def auth(self) -> None:
            self.auth_calls += 1

        def get_headers(self) -> dict[str, str]:
            return {"Authorization": "Bearer refreshed"}

    auth = Reauth()
    hook_observations = []

    def record_hook(response, *args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        hook_observations.append((response.status_code, response.request.url))
        return response

    hook = mocker.Mock(side_effect=record_hook)
    auth.session.hooks["response"].append(hook)
    requests_mock.get(
        "http://example.org/protected",
        [
            {"status_code": 401},
            {
                "status_code": 302,
                "headers": {"Location": "/target"},
            },
        ],
    )
    requests_mock.get("http://example.org/target", status_code=401)

    response = auth.session.get("http://example.org/protected")

    assert response.status_code == 401
    assert auth.auth_calls == 1
    assert hook.call_count == 3
    assert hook_observations == [
        (302, "http://example.org/protected"),
        (401, "http://example.org/target"),
        (401, "http://example.org/target"),
    ]
    assert [
        (request.url, request.headers.get("Authorization"))
        for request in requests_mock.request_history
    ] == [
        ("http://example.org/protected", None),
        ("http://example.org/protected", "Bearer refreshed"),
        ("http://example.org/target", "Bearer refreshed"),
    ]
    assert auth.session.hooks["response"] == [auth.reauth, hook]
