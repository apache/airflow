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
"""Capabilities for tests of how durable execution treats them."""

from __future__ import annotations

from dataclasses import dataclass, field

# RunContext and ModelRequestContext are runtime imports: pydantic-ai resolves a durable
# operation's type hints when it builds the agent.
from pydantic_ai import RunContext  # noqa: TC002
from pydantic_ai.capabilities import AbstractCapability, durable_operation
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models import ModelRequestContext  # noqa: TC002
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.toolsets import FunctionToolset


class IdlessLedger(AbstractCapability):
    """A capability with a durable operation and no id, which pydantic-ai refuses under durable execution."""

    @durable_operation(name="accrue")
    async def accrue(self, ctx: RunContext, amount: int) -> int:
        return amount


@dataclass
class LedgerRenamedForRun(AbstractCapability):
    """A capability with a durable operation whose ``for_run`` changes its id, which pydantic-ai refuses once running."""

    id: str | None = "ledger"

    @durable_operation(name="accrue")
    async def accrue(self, ctx: RunContext, amount: int) -> int:
        return amount

    async def for_run(self, ctx: RunContext) -> LedgerRenamedForRun:
        return LedgerRenamedForRun(id="ledger-for-run")


@dataclass
class ToolsetRebuiltForRun(AbstractCapability):
    """A capability whose ``for_run`` builds its toolset again under the same id, which pydantic-ai refuses once running."""

    def get_toolset(self) -> FunctionToolset:
        def ping() -> str:
            return "pong"

        return FunctionToolset(tools=[ping], id="rebuilt")

    async def for_run(self, ctx: RunContext) -> ToolsetRebuiltForRun:
        return ToolsetRebuiltForRun()


@dataclass
class RouterThatActs(AbstractCapability):
    """A durable ``before_model_request`` that acts, then routes to a model pydantic-ai refuses once running."""

    id: str | None = "router"
    actions: list[str] = field(default_factory=list)

    @durable_operation(name="route")
    async def before_model_request(
        self, ctx: RunContext, request_context: ModelRequestContext
    ) -> ModelRequestContext:
        self.actions.append("routed")
        request_context.model = FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("done")]))
        return request_context
