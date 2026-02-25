"""
Mechanisms for authentication and authorization.
"""

from typing import Any, Dict

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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

        # prevent infinite recursion
        if getattr(r.request, "_reauth_attempted", False):
            return r

        self.auth()

        headers = self.get_headers()

        print(headers)

        new_request = r.request.copy()

        # critical: explicitly inject Authorization
        new_request.headers["Authorization"] = headers["Authorization"]

        new_request._reauth_attempted = True

        # prevent recursion via hooks
        new_request.hooks = {}

        return self.session.send(new_request, verify=False)
