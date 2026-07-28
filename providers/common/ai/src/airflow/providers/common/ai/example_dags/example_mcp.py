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
"""Example DAGs demonstrating MCP server integration with AgentOperator."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.mcp import MCPToolset
from airflow.providers.common.compat.sdk import dag

# ---------------------------------------------------------------------------
# 1. MCPToolset with Airflow connection (recommended for production)
# ---------------------------------------------------------------------------


# [START howto_toolset_mcp_connection]
@dag(tags=["example"])
def example_mcp_toolset():
    """Use an MCP server configured via an Airflow connection."""
    AgentOperator(
        task_id="mcp_agent",
        prompt="What tools are available? Run the hello tool.",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a helpful assistant with access to MCP tools.",
        toolsets=[
            MCPToolset(mcp_conn_id="my_mcp_server"),
        ],
    )


# [END howto_toolset_mcp_connection]

example_mcp_toolset()


# ---------------------------------------------------------------------------
# 2. Multiple MCP servers with tool prefixes
# ---------------------------------------------------------------------------


# [START howto_toolset_mcp_multiple]
@dag(tags=["example"])
def example_mcp_multiple_servers():
    """Combine multiple MCP servers with prefixes to avoid tool name collisions."""
    AgentOperator(
        task_id="multi_mcp_agent",
        prompt="Get the weather in London and run a Python calculation: 2**10",
        llm_conn_id="pydanticai_default",
        system_prompt="You have access to weather and code execution tools.",
        toolsets=[
            MCPToolset(mcp_conn_id="weather_mcp", tool_prefix="weather"),
            MCPToolset(mcp_conn_id="code_runner_mcp", tool_prefix="code"),
        ],
    )


# [END howto_toolset_mcp_multiple]

example_mcp_multiple_servers()


# ---------------------------------------------------------------------------
# 3. Direct PydanticAI MCP toolsets (no Airflow connection needed)
# ---------------------------------------------------------------------------
# AgentOperator accepts any PydanticAI AbstractToolset, including MCPToolset
# directly. Use this for prototyping or when you want full PydanticAI control.
#
#   from fastmcp.client.transports import StdioTransport
#   from pydantic_ai.mcp import MCPToolset
#
#   AgentOperator(
#       task_id="direct_mcp",
#       prompt="What tools are available?",
#       llm_conn_id="pydanticai_default",
#       toolsets=[
#           MCPToolset("http://localhost:3001/mcp"),
#           MCPToolset(StdioTransport(command="uvx", args=["mcp-run-python"])),
#       ],
#   )


# ---------------------------------------------------------------------------
# 4. Stdio server with a minted secret in the subprocess environment
# ---------------------------------------------------------------------------
# For local stdio MCP servers that read credentials from their own environment
# (e.g. a server that needs a Splunk API key), pass env_provider instead of
# storing the secret in the connection's static Extra.env. It is resolved at
# task-execution time -- never baked into the serialized DAG -- and merged
# over Extra.env, with env_provider's keys winning on conflicts. Here the
# secret lives in a *different* connection (the Splunk one, not this MCP
# server's own connection) -- something a static Extra.env cannot express.


# [START howto_toolset_mcp_env_provider]
def _mint_splunk_env() -> dict[str, str]:
    from airflow.providers.common.compat.sdk import BaseHook

    password = BaseHook.get_connection("splunk_default").password
    if not password:
        raise ValueError("splunk_default connection has no password set")
    return {"SPLUNK_API_KEY": password}


@dag(tags=["example"])
def example_mcp_stdio_env_provider():
    """Use a stdio MCP server whose subprocess needs a secret from another connection."""
    AgentOperator(
        task_id="stdio_env_agent",
        prompt="Investigate the ticket and summarize findings.",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a support triage agent with access to MCP tools.",
        toolsets=[
            MCPToolset(mcp_conn_id="spacefarer_mcp", env_provider=_mint_splunk_env),
        ],
    )


# [END howto_toolset_mcp_env_provider]

example_mcp_stdio_env_provider()
