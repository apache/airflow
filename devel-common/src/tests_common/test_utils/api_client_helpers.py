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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def generate_access_token(username: str, password: str, host: str) -> str:
    """
    Generate valid access token for the given username and password.

    Note: API server is currently using Simple Auth Manager.

    :param username: The username to use for the login
    :param password: The password to use for the login
    :param host: The host to use for the login
    :return: The access token
    """
    Retry.DEFAULT_BACKOFF_MAX = 32
    # retry for rate limit errors (429) and server errors (500, 502, 503, 504)
    retry = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    # Backoff Retry Formula: min(1 × (2^(retry - 1)), 32) seconds
    # 1 + 2 + 4 + 8 + 16 + 32 + 32 + 32 + 32 + 32 = 191 sec (~3.2 min)
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    url = f"http://{host}/auth/token"
    login_response = session.post(
        url,
        json={"username": username, "password": password},
    )
    access_token = login_response.json().get("access_token")

    assert access_token, f"Failed to get JWT token from redirect url {url} with status code {login_response}"
    return access_token
