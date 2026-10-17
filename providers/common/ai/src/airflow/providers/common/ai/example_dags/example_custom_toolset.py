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
"""Example Dag: a toolset of your own, written by subclassing ``AirflowToolset``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import Field, TypeAdapter
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_core import SchemaValidator
from typing_extensions import TypedDict

from airflow.providers.common.ai.utils.toolset_base import AirflowToolset
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from pydantic_ai import RunContext


# [START howto_toolset_custom_class]
class PackageArgs(TypedDict):
    """The arguments of get_package."""

    # A pattern keeps the model's input to a valid name, so it cannot reach another path on the host.
    name: Annotated[
        str,
        Field(
            description="The distribution name, such as apache-airflow.",
            pattern=r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$",
        ),
    ]


class ReleaseArgs(PackageArgs):
    """The arguments of get_release."""

    version: Annotated[
        str, Field(description="An exact version, such as 3.1.0.", pattern=r"^[A-Za-z0-9][A-Za-z0-9.!+_-]*$")
    ]


# Each tool's description, and the adapter that gives its arguments' JSON schema and validator.
TOOLS: dict[str, tuple[str, TypeAdapter[Any]]] = {
    "get_package": (
        "Look up a package on PyPI: its latest version, summary and supported Python versions.",
        TypeAdapter(PackageArgs),
    ),
    "get_release": (
        "Look up one release of a package on PyPI: what it depends on and when it was uploaded.",
        TypeAdapter(ReleaseArgs),
    ),
}


class PyPIToolset(AirflowToolset):
    """Look packages up on PyPI, or a mirror of its JSON API, through an HTTP connection."""

    # AgentOperator renders these attributes with Jinja for each task instance.
    agent_template_fields: Sequence[str] = ("http_conn_id",)

    def __init__(self, http_conn_id: str = "pypi") -> None:
        self.http_conn_id = http_conn_id

    @property
    def id(self) -> str:
        return f"pypi-{self.http_conn_id}"

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        return {
            name: ToolsetTool(
                toolset=self,
                tool_def=ToolDefinition(
                    name=name, description=description, parameters_json_schema=args.json_schema()
                ),
                max_retries=ctx.max_retries,
                args_validator=SchemaValidator(args.core_schema),
            )
            for name, (description, args) in TOOLS.items()
        }

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> dict[str, Any]:
        if name == "get_package":
            path = tool_args["name"]
        elif name == "get_release":
            path = f"{tool_args['name']}/{tool_args['version']}"
        else:
            raise ValueError(f"Unknown tool: {name!r}")

        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        # HttpHook.run blocks. run_blocking runs it in a thread, one hook call at a time, so
        # give it a timeout: a stalled request would hold up every other blocking call.
        response = await self.run_blocking(
            hook.run, f"pypi/{path}/json", extra_options={"check_response": False, "timeout": 30}
        )
        if response.status_code == 404:
            # The model named something that does not exist; it reads this and can try again.
            raise ModelRetry(f"PyPI has no {path!r}. Check the name and version.")
        # Any other failure is not the model's to fix, so it fails the task.
        response.raise_for_status()

        # The full document lists every file of every release. Return what the model needs.
        document = response.json()
        info = document["info"]
        if name == "get_package":
            return {key: info[key] for key in ("name", "version", "summary", "requires_python")}
        return {
            "version": info["version"],
            "requires_python": info["requires_python"],
            "requires_dist": info["requires_dist"],
            "yanked": info["yanked"],
            "uploaded": document["urls"][0]["upload_time_iso_8601"] if document["urls"] else None,
        }


# [END howto_toolset_custom_class]


# [START howto_toolset_custom_agent]
@dag(tags=["example"])
def example_custom_toolset():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You answer questions about Python packages. Look them up; do not guess.",
        toolsets=[PyPIToolset(http_conn_id="pypi")],
    )
    def check_package(package: str) -> str:
        return f"What is the latest release of {package}, and which Python versions does it support?"

    check_package("apache-airflow-providers-common-ai")


# [END howto_toolset_custom_agent]

example_custom_toolset()
