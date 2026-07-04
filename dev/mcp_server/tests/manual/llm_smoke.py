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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pydantic-ai-slim[mcp,anthropic,openai]",
#     "fastmcp>=3.0,<4.0",
# ]
# ///
"""Manual smoke test: connect a real LLM agent to the Airflow dev MCP server over stdio.

Not run by CI or plain pytest -- requires a real LLM API key. Run directly with `uv`,
which installs this script's own dependencies in an ephemeral environment (separate
from dev/mcp_server's own dependencies):

    ANTHROPIC_API_KEY=... uv run dev/mcp_server/tests/manual/llm_smoke.py

Pick a different provider/model with AIRFLOW_MCP_LLM_MODEL, using pydantic-ai's
`<provider>:<model>` convention (e.g. `openai:gpt-5`); the corresponding API key env var
(e.g. OPENAI_API_KEY) is read by pydantic-ai as usual for that provider.

Requires a reachable Airflow instance (e.g. a running Breeze) for the server to talk to --
see dev/mcp_server/README.md for AIRFLOW_API_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPToolset, StdioTransport

MCP_SERVER_DIR = Path(__file__).resolve().parents[2]
DEFAULT_MODEL = "anthropic:claude-sonnet-4-5"
PROMPT = "List the Dags currently known to this Airflow instance, and mention if any have import errors."


async def main() -> None:
    model = os.environ.get("AIRFLOW_MCP_LLM_MODEL", DEFAULT_MODEL)
    transport = StdioTransport(
        command="uv", args=["run", "--project", str(MCP_SERVER_DIR), "airflow-dev-mcp"]
    )
    agent = Agent(model=model, toolsets=[MCPToolset(client=transport)])

    print(f"Connecting to {MCP_SERVER_DIR} over stdio, using model {model!r}...", file=sys.stderr)
    result = await agent.run(PROMPT)
    print(result.output)


if __name__ == "__main__":
    asyncio.run(main())
