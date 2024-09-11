#
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
"""API Client that allows interacting with Airflow API."""

from __future__ import annotations

from airflow import api
from airflow.api.client.local_client import Client


def get_current_api_client() -> Client:
    """Return current API Client based on current Airflow configuration."""
    auth_backends = api.load_auth()
    session = None
    for backend in auth_backends:
        session_factory = getattr(backend, "create_client_session", None)
        if session_factory:
            session = session_factory()
        api_client = Client(
            auth=getattr(backend, "CLIENT_AUTH", None),
            session=session,
        )
    return api_client
