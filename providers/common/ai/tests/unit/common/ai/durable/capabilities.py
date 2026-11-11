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

# A runtime import: pydantic-ai resolves a durable operation's type hints when it builds the agent.
from pydantic_ai import RunContext  # noqa: TC002
from pydantic_ai.capabilities import AbstractCapability, durable_operation


class IdlessLedger(AbstractCapability):
    """A capability with a durable operation and no id, which pydantic-ai refuses under durable execution."""

    @durable_operation(name="accrue")
    async def accrue(self, ctx: RunContext, amount: int) -> int:
        return amount
