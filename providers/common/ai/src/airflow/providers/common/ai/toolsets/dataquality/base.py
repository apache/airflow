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
"""Abstract base for data-quality toolsets used by LLMDataQualityOperator."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.ai.utils.dataquality.models import DQCheckInput

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

_LIST_CHECKS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {},
}


class BaseDQToolset(AbstractToolset[Any]):
    """
    Abstract base for all data-quality toolsets.

    Subclasses implement one of two modes:

    * ``"execute"`` — the toolset runs checks against a live data source and
      the operator expects a
      :class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport` as the
      agent's final output.  The task fails when any check does not pass.
    * ``"generate"`` — the toolset produces a configuration artifact (e.g. a
      SODA YAML or Great Expectations suite JSON) that a downstream operator
      can feed to an external DQ framework.  The operator returns the config
      string as its XCom value without performing pass/fail gating.

    :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator`
    locates the DQ toolset in its ``toolsets`` list by checking
    ``isinstance(toolset, BaseDQToolset)`` and reads :attr:`output_mode` to
    decide its post-processing behaviour.

    All subclasses must call :meth:`set_checks` before the agent runs —
    the operator does this automatically.
    """

    def __init__(self) -> None:
        self._checks: list[DQCheckInput] = []
        self._planning_mode: bool = False

    @property
    def output_mode(self) -> Literal["execute", "generate"]:
        """
        Return the execution mode for this toolset.

        ``"execute"`` means the toolset runs checks and the operator gates on
        pass/fail.  ``"generate"`` means the toolset produces a config file
        and the operator returns it as output.
        """
        raise NotImplementedError

    def set_checks(self, checks: list[DQCheckInput]) -> None:
        """Store *checks* so the ``list_checks`` tool can return them to the agent."""
        self._checks = checks

    # ------------------------------------------------------------------
    # AbstractToolset interface
    # ------------------------------------------------------------------

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tool_def = ToolDefinition(
            name="list_checks",
            description="List the data-quality checks requested by the user.",
            parameters_json_schema=_LIST_CHECKS_SCHEMA,
            sequential=True,
        )
        return {
            "list_checks": ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        }

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name == "list_checks":
            return self._list_checks()
        raise ValueError(f"Unknown tool: {name!r}")

    # ------------------------------------------------------------------
    # Tool implementation
    # ------------------------------------------------------------------

    def _list_checks(self) -> str:
        return json.dumps(
            [
                {
                    "name": c.name,
                    "description": c.description,
                    "has_fixed_validator": c.validator is not None,
                    "row_level": bool(getattr(c.validator, "_row_level", False)),
                }
                for c in self._checks
            ]
        )
