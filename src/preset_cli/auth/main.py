"""
Mechanisms for authentication and authorization.
"""

import logging
from typing import Any, Dict

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

_logger = logging.getLogger(__name__)


class Auth:  # pylint: disable=too-few-public-methods
    """
    An authentication/authorization mechanism.
    """

    def __init__(self):
        self.session = Session()
        self.session.hooks["response"].append(self.reauth)

        retries = Retry(
            total=3,  # max retries count
            backoff_factor=1,  # delay factor between attempts
            respect_retry_after_header=True,
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_headers(self) -> Dict[str, str]:
        """
        Return headers for auth.
        """
        return {}

    def auth(self) -> None:
        """
        Perform authentication, fetching JWT tokens, CSRF tokens, cookies, etc.
        """
        raise NotImplementedError("Must be implemented for reauthorizing")

    # pylint: disable=invalid-name, unused-argument
    def reauth(self, r: Response, *args: Any, **kwargs: Any) -> Response:
        """
        Catch 401 and re-auth.
        """
        if r.status_code != 401:
            return r

        _logger.debug("Token expired. Re-authenticating...")

        try:
            self.auth()
        except NotImplementedError:
            return r

        headers = self.get_headers()
        request = r.request.copy()
        request.hooks = request.hooks.copy()
        request.hooks["response"] = [
            hook
            for hook in request.hooks["response"]
            if not (
                getattr(hook, "__self__", None) is self
                and getattr(hook, "__func__", None)
                is getattr(self.reauth, "__func__", None)
            )
        ]
        for target in (self.session.headers, request.headers):
            target.pop("Authorization", None)
            target.pop("X-CSRFToken", None)
            target.update(headers)

        # A prepared request does not pick up cookies added to the session
        # after it was prepared.  Legacy auth deliberately supports only the
        # ordinary Session cookie-jar flow here, not raw Cookie provenance.
        if getattr(self, "_using_legacy_auth", False):
            request.headers.pop("Cookie", None)
            request.prepare_cookies(self.session.cookies)

        return self.session.send(request, verify=False)
