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
from __future__ import annotations

import httpx
import httpx2
import pytest

from tests_common.test_utils.in_process_taskrun import resolve_sdk_httpx


@pytest.mark.parametrize("expected", [httpx, httpx2])
def test_resolve_sdk_httpx_follows_the_client_base(monkeypatch, expected):
    class FakeClient(expected.Client):
        pass

    monkeypatch.setattr("airflow.sdk.api.client.Client", FakeClient, raising=True)

    assert resolve_sdk_httpx() is expected


def test_resolve_sdk_httpx_rejects_an_unknown_stack(monkeypatch):
    class FakeClient:
        pass

    monkeypatch.setattr("airflow.sdk.api.client.Client", FakeClient, raising=True)

    with pytest.raises(RuntimeError, match="neither httpx nor httpx2"):
        resolve_sdk_httpx()
