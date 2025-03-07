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

import httpx

from airflow.sdk.api.client import Client
from airflow.sdk.execution_time.comms import BundleInfo

FAKE_BUNDLE = BundleInfo(name="anything", version="any")


def make_client(transport: httpx.MockTransport) -> Client:
    """Get a client with a custom transport."""
    return Client(base_url="test://server", token="", transport=transport)


def make_client_w_dry_run() -> Client:
    """Get a client with dry_run enabled."""
    return Client(base_url=None, dry_run=True, token="")


def make_client_w_responses(responses: list[httpx.Response]) -> Client:
    """Get a client with custom responses."""

    def handle_request(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    return Client(
        base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
    )
