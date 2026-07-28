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
"""Version-tolerant helpers for building pydantic-ai ``ToolDefinition`` objects."""

from __future__ import annotations

import dataclasses
from typing import Any

from pydantic_ai.tools import ToolDefinition

# ``ToolDefinition.return_schema`` is newer than the provider's pydantic-ai
# floor. Detect it once so callers can include the kwarg only when supported,
# rather than raising ``TypeError`` on older installs.
_SUPPORTS_RETURN_SCHEMA = any(f.name == "return_schema" for f in dataclasses.fields(ToolDefinition))


def return_schema_kwargs(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Return ``{"return_schema": schema}`` when pydantic-ai supports the field, else ``{}``.

    ``return_schema`` lets CodeMode (the Monty sandbox) render a typed function
    signature for a tool (``-> str``) instead of ``-> Any``, which helps the
    model write correct code. It has no effect outside code mode.

    :param schema: A JSON Schema fragment describing the tool's return value.
    """
    if _SUPPORTS_RETURN_SCHEMA:
        return {"return_schema": schema}
    return {}
