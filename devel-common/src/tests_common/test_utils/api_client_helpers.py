# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import re
from time import time
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def generate_jwt_token(username: str, password: str, host_port: str) -> str:
    """
    Get the JWT token.

    Note: API server is still using FAB Auth Manager.

    Steps:
    1. Get the login page to get the csrf token
        - The csrf token is in the hidden input field with id "csrf_token"
    2. Login with the username and password
        - Must use the same session to keep the csrf token session
    3. Extract the JWT token from the redirect url
        - Expected to have a connection error
        - The redirect url should have the JWT token as a query parameter

    :param username: The username
    :param password: The password
    :param host_port: The hostname with port
    :return: The JWT token
    """
    # get csrf token from login page
    retry = Retry(total=5, backoff_factor=10)
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    get_login_form_response = session.get(f"http://{host_port}/auth/login")
    csrf_token = re.search(
        r'<input id="csrf_token" name="csrf_token" type="hidden" value="(.+?)">',
        get_login_form_response.text,
    )
    assert csrf_token, "Failed to get csrf token from login page"
    csrf_token_str = csrf_token.group(1)
    assert csrf_token_str, "Failed to get csrf token from login page"
    # login with form data
    login_response = session.post(
        f"http://{host_port}/auth/login",
        data={
            "username": username,
            "password": password,
            "csrf_token": csrf_token_str,
        },
    )
    redirect_url = login_response.url
    # ensure redirect_url is a string
    redirect_url_str = str(redirect_url) if redirect_url is not None else ""
    assert "/?token" in redirect_url_str, f"Login failed with redirect url {redirect_url_str}"
    parsed_url = urlparse(redirect_url_str)
    query_params = parse_qs(str(parsed_url.query))
    jwt_token_list = query_params.get("token")
    jwt_token = jwt_token_list[0] if jwt_token_list else None
    assert jwt_token, f"Failed to get JWT token from redirect url {redirect_url_str}"
    return jwt_token


class RefreshJwtAdapter(HTTPAdapter):
    """Adapter to refresh JWT token on 401/403 response."""

    def __init__(self, username: str, password: str, host_port: str, **kwargs):
        self.username = username
        self.password = password
        self.host_port = host_port
        super().__init__(**kwargs)

    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)
        if response.status_code in (401, 403):
            # Refresh token and update the Authorization header with retry logic.
            attempts = 0
            jwt_token = None
            while attempts < 5:
                try:
                    jwt_token = generate_jwt_token(self.username, self.password, self.host_port)
                    break
                except Exception:
                    attempts += 1
                    time.sleep(1)
            if jwt_token is None:
                raise Exception("Failed to refresh JWT token after 5 attempts")
            request.headers["Authorization"] = f"Bearer {jwt_token}"
            response = super().send(request, **kwargs)
        return response
