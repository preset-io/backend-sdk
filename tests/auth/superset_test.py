"""
Test username:password authentication mechanism.
"""

from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.superset import (
    SupersetJWTAuth,
    UsernamePasswordAuth,
    get_access_token,
)


def test_username_password_auth(requests_mock: Mocker) -> None:
    """
    Tests for the username/password authentication mechanism.
    """
    csrf_token = "CSFR_TOKEN"
    access_token = "ACCESS_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": csrf_token},
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        json={"access_token": access_token},
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.get_headers() == {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
    }


def test_username_password_auth_no_csrf(requests_mock: Mocker) -> None:
    """
    When the security API returns no CSRF token, only the Bearer header is set.
    """
    access_token = "ACCESS_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": None},
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        json={"access_token": access_token},
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.get_headers() == {"Authorization": f"Bearer {access_token}"}


def test_username_password_auth_legacy_fallback(requests_mock: Mocker) -> None:
    """
    When the security API is unavailable, fall back to the legacy
    HTML-scraping login flow.
    """
    csrf_token = "LEGACY_CSRF"
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        status_code=404,
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text=(
            f'<html><body><input id="csrf_token" name="csrf_token" '
            f'value="{csrf_token}" /></body></html>'
        ),
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    # no JWT is obtained in legacy mode; auth is carried by cookies + CSRF
    assert auth.token is None
    assert auth.get_headers() == {"X-CSRFToken": csrf_token}
    assert "Authorization" not in auth.session.headers


def test_username_password_auth_legacy_fallback_no_csrf(
    requests_mock: Mocker,
) -> None:
    """
    In the legacy flow, when the login page exposes no CSRF token input, the
    credentials are still posted (relying on cookies) and no CSRF header is set.
    """
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        status_code=404,
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text="<html><body>no csrf here</body></html>",
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    # pylint: disable=use-implicit-booleaness-not-comparison
    assert auth.token is None
    assert auth.csrf_token is None
    assert auth.get_headers() == {}
    assert "X-CSRFToken" not in auth.session.headers


def test_username_password_auth_reauth(requests_mock: Mocker) -> None:
    """
    On a 401, ``reauth`` re-runs the login flow and the retried request
    carries a fresh Bearer token.
    """
    csrf_token = "CSFR_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": csrf_token},
    )
    # the login endpoint hands out a new token on each call
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        [
            {"json": {"access_token": "TOKEN_1"}},
            {"json": {"access_token": "TOKEN_2"}},
        ],
    )
    # the protected resource is expired once, then succeeds
    requests_mock.get(
        "https://superset.example.org/api/v1/me/",
        [
            {"status_code": 401},
            {"status_code": 200, "json": {"result": "ok"}},
        ],
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.token == "TOKEN_1"
    auth.session.headers["Referer"] = "https://superset.example.org/"

    response = auth.session.get(
        "https://superset.example.org/api/v1/me/",
        headers=auth.get_headers(),
    )
    assert response.status_code == 200
    # the token was refreshed and the retried request used it
    assert auth.token == "TOKEN_2"
    last_request = requests_mock.last_request
    assert last_request is not None
    assert last_request.headers["Authorization"] == "Bearer TOKEN_2"
    assert auth.session.headers["Referer"] == "https://superset.example.org/"
    login_requests = [
        request
        for request in requests_mock.request_history
        if request.url.endswith("/api/v1/security/login")
    ]
    assert len(login_requests) == 2
    assert all(request.headers.get("Referer") is None for request in login_requests)


def test_username_password_auth_subrequests_do_not_recurse(
    requests_mock: Mocker,
) -> None:
    """API and redirected legacy 401s do not recursively reauthenticate."""
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        status_code=401,
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text="<html><body>no csrf here</body></html>",
    )
    requests_mock.post(
        "https://superset.example.org/login/",
        status_code=302,
        headers={"Location": "/legacy-finished/"},
    )
    requests_mock.get("https://superset.example.org/legacy-finished/", status_code=401)

    UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )

    assert (
        len(
            [
                request
                for request in requests_mock.request_history
                if request.url.endswith("/api/v1/security/login")
            ],
        )
        == 1
    )
    assert (
        len(
            [
                request
                for request in requests_mock.request_history
                if request.url.endswith("/legacy-finished/")
            ],
        )
        == 1
    )


def test_username_password_auth_fallback_clears_api_state_and_refreshes_cookie(
    requests_mock: Mocker,
) -> None:
    """Legacy replay uses the refreshed Session cookie and no stale API auth."""
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        [
            {"json": {"access_token": "TOKEN_1"}},
            {"json": {"access_token": "TOKEN_2"}},
        ],
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        [{"json": {"result": "CSRF_1"}}, {"status_code": 500}],
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text='<input id="csrf_token" value="LEGACY_CSRF">',
    )
    auth = None

    def legacy_login(request, context):  # type: ignore[no-untyped-def]
        del request, context
        assert auth is not None
        auth.session.cookies.set(
            "session",
            "refreshed",
            domain="superset.example.org",
            path="/",
        )
        return ""

    requests_mock.post("https://superset.example.org/login/", text=legacy_login)
    requests_mock.get(
        "https://superset.example.org/api/v1/me/",
        [{"status_code": 401}, {"status_code": 200, "json": {"ok": True}}],
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    auth.session.headers.update(auth.get_headers())
    auth.session.headers["X-Unrelated"] = "keep"
    auth.session.cookies.set(
        "session",
        "stale",
        domain="superset.example.org",
        path="/",
    )

    response = auth.session.get(
        "https://superset.example.org/api/v1/me/",
        headers={"X-Request": "keep"},
    )

    assert response.status_code == 200
    assert auth.token is None
    assert auth.csrf_token == "LEGACY_CSRF"
    assert "Authorization" not in auth.session.headers
    assert auth.session.headers["X-CSRFToken"] == "LEGACY_CSRF"
    assert auth.session.headers["X-Unrelated"] == "keep"
    last_request = requests_mock.last_request
    assert last_request is not None
    assert last_request.headers["Cookie"] == "session=refreshed"
    assert "Authorization" not in last_request.headers
    assert last_request.headers["X-CSRFToken"] == "LEGACY_CSRF"
    assert last_request.headers["X-Request"] == "keep"


def test_username_password_auth_preserves_session_auth_and_response_hooks(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """Custom Session auth and response hooks remain usable."""
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        [{"json": {"access_token": "TOKEN_1"}}, {"json": {"access_token": "TOKEN_2"}}],
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        [{"json": {"result": "CSRF_1"}}, {"json": {"result": "CSRF_2"}}],
    )
    requests_mock.get("https://superset.example.org/api/v1/me/", status_code=200)

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )

    def add_session_auth(request):  # type: ignore[no-untyped-def]
        request.headers["X-Session-Auth"] = "yes"
        return request

    session_auth = mocker.Mock(side_effect=add_session_auth)
    hook = mocker.Mock(side_effect=lambda response, *args, **kwargs: response)
    auth.session.auth = session_auth
    auth.session.hooks["response"].append(hook)

    auth.auth()
    response = auth.session.get("https://superset.example.org/api/v1/me/")

    assert response.status_code == 200
    assert auth.session.auth is session_auth
    assert auth.session.hooks["response"][-1] is hook
    assert session_auth.call_count == 3
    assert hook.call_count == 3
    last_request = requests_mock.last_request
    assert last_request is not None
    assert last_request.headers["X-Session-Auth"] == "yes"


def test_superset_jwt_unsupported_reauth_returns_original_401(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """JWT auth does not fetch another CSRF token when refresh is unsupported."""
    requests_mock.get("https://example.org/api/v1/me/", status_code=401)
    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))
    get_csrf = mocker.patch.object(auth, "get_csrf_token", return_value="CSRF")

    response = auth.session.get(
        "https://example.org/api/v1/me/",
        headers=auth.get_headers(),
    )

    assert response.status_code == 401
    get_csrf.assert_called_once_with("my-token")


def test_jwt_auth_superset(mocker: MockerFixture) -> None:
    """
    Test the ``SupersetJWTAuth`` authentication mechanism for a Superset tenant.
    """
    mocker.patch(
        "preset_cli.auth.superset.get_csrf_token",
        return_value="myCSRFToken",
    )
    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))

    assert auth.get_headers() == {
        "Authorization": "Bearer my-token",
        "X-CSRFToken": "myCSRFToken",
    }


def test_get_access_token(requests_mock: Mocker) -> None:
    """
    Test the ``get_access_token`` helper.
    """
    requests_mock.post(
        "https://example.org/api/v1/security/login",
        json={"access_token": "my-token"},
    )

    session = Session()
    token = get_access_token(
        session,
        URL("https://example.org/"),
        "admin",
        "password123",
        "db",
    )
    assert token == "my-token"
    last_request = requests_mock.last_request
    assert last_request is not None
    assert last_request.json() == {
        "username": "admin",
        "password": "password123",
        "provider": "db",
    }


def test_get_csrf_token(requests_mock: Mocker) -> None:
    """
    Test the ``get_csrf_token`` helper.
    """
    requests_mock.get(
        "https://example.org/api/v1/security/csrf_token/",
        json={"result": "myCSRFToken"},
    )

    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))
    csrf_token = auth.get_csrf_token("my-token")
    assert csrf_token == "myCSRFToken"
    last_request = requests_mock.last_request
    assert last_request is not None
    assert last_request.headers["Authorization"] == "Bearer my-token"
