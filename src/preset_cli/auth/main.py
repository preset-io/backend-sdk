"""
Mechanisms for authentication and authorization.
"""

from typing import Dict

import requests


class Auth:  # pylint: disable=too-few-public-methods
    """
    An authentication/authorization mechanism.
    """

    def __init__(self):
        self.session = requests.Session()
        self.headers = {}

    def get_session(self) -> requests.Session:
        """
        Return a session.
        """
        return self.session

    def get_headers(self) -> Dict[str, str]:  # pylint: disable=no-self-use
        """
        Return headers for auth.
        """
        return self.headers
