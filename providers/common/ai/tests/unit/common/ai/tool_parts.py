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
"""
Classify the message parts that answer a tool call, across pydantic-ai versions.

Before pydantic-ai 2.60 a tool the model must call again answers with a ``RetryPromptPart``; from
2.60 it answers with the call's own ``ToolReturnPart`` whose ``outcome`` is ``"retried"``.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai.messages import RetryPromptPart, ToolReturnPart


def is_tool_retry(part: Any) -> bool:
    return isinstance(part, RetryPromptPart) or (
        isinstance(part, ToolReturnPart) and getattr(part, "outcome", None) == "retried"
    )


def is_tool_result(part: Any) -> bool:
    return isinstance(part, ToolReturnPart) and not is_tool_retry(part)
