"""
Test JWT auth.
"""

from preset_cli.auth.jwt import JWTAuth


def test_jwt_auth() -> None:
    """
    Test the ``JWTAuth`` authentication mechanism.
    """
    auth = JWTAuth("my-token")
    assert auth.get_headers() == {"Authorization": "Bearer my-token"}
