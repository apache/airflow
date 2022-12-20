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

import base64
import urllib.parse

import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from airflow.hooks.base import BaseHook


def create_session_for_connection(connection_name: str):
    print(f"Creating session for connection named {connection_name}")

    # disable insecure HTTP requests warnings
    urllib3.disable_warnings(InsecureRequestWarning)

    conn = BaseHook.get_connection(connection_name)
    extras = conn.extra_dejson
    token = extras.get("token")
    client_id = extras.get("client_id")
    client_secret = ""
    if not client_id:
        client_id = "sas.cli"
    else:
        client_secret = extras.get("client_secret")  # type: ignore

    if not token:
        token = _create_session_for_user(conn.host, conn.login, conn.password, client_id, client_secret)

    session = requests.Session()

    # set up standard headers
    session.headers.update({"Authorization": f"bearer {token}"})
    session.headers.update({"Accept": "application/json"})
    session.headers.update({"Content-Type": "application/json"})

    # set to false if using self signed certs
    session.verify = False

    # prepend the root url for all operations on the session, so that consumers can just provide
    # resource uri without the protocol and host
    root_url = conn.host
    session.get = lambda *args, **kwargs: requests.Session.get(  # type: ignore
        session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
    )
    session.post = lambda *args, **kwargs: requests.Session.post(  # type: ignore
        session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
    )
    session.put = lambda *args, **kwargs: requests.Session.put(  # type: ignore
        session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
    )
    return session


def _create_session_for_user(
    root_url: str, userid: str, password: str, client_id: str = "sas.cli", client_secret: str = ""
):
    # base 64 encode the api client auth and pass in authorization header
    auth_str = f"{client_id}:{client_secret}"
    auth_bytes = auth_str.encode("ascii")
    auth_header = base64.b64encode(auth_bytes).decode("ascii")
    my_headers = {"Authorization": f"Basic {auth_header}"}

    payload = {"grant_type": "password", "username": userid, "password": password}

    print("Get oauth token")
    response = requests.post(
        f"{root_url}/SASLogon/oauth/token", data=payload, verify=False, headers=my_headers
    )

    if response.status_code != 200:
        raise RuntimeError(f"Get token failed: {response.text}")

    r = response.json()
    token = r["access_token"]
    return token
