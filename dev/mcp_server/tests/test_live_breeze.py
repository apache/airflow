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
"""Manual integration test against a real, running Breeze Airflow instance.

Not run by CI or by a plain `pytest` invocation -- opt in with:

    AIRFLOW_MCP_LIVE_TESTS=1 uv run --project dev/mcp_server pytest dev/mcp_server/tests/test_live_breeze.py

Requires `breeze start-airflow` (or equivalent) already running and reachable at
AIRFLOW_API_URL (default http://localhost:28080).
"""

from __future__ import annotations

import os

import pytest
from fastmcp import Client

from airflow_mcp_server.server import mcp

pytestmark = pytest.mark.skipif(
    not os.environ.get("AIRFLOW_MCP_LIVE_TESTS"),
    reason="Set AIRFLOW_MCP_LIVE_TESTS=1 to run against a live Breeze instance.",
)


async def test_get_version_round_trip():
    async with Client(mcp) as c:
        result = await c.call_tool("get_version", {})
    assert "version" in result.data


async def test_list_dags_round_trip():
    async with Client(mcp) as c:
        result = await c.call_tool("list_dags", {"limit": 5})
    assert "dags" in result.data
    assert "total_entries" in result.data


async def test_list_import_errors_round_trip():
    async with Client(mcp) as c:
        result = await c.call_tool("list_import_errors", {"limit": 5})
    assert "import_errors" in result.data
