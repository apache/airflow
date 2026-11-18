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
"""The ``tool_prefix`` handling shared by the toolsets that namespace their tool names."""

from __future__ import annotations


def validate_tool_prefix(tool_prefix: str) -> None:
    """Raise ``ValueError`` if a non-empty ``tool_prefix`` is not a valid Python identifier."""
    if tool_prefix and not tool_prefix.isidentifier():
        # The prefixed names are rendered as Python function signatures under
        # code mode, so a name that is not an identifier breaks there.
        raise ValueError(f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}.")


class ToolPrefixMixin:
    """Build, and map back from, the tool names and the toolset id that a ``tool_prefix`` produces."""

    _tool_prefix: str = ""

    tool_name_conflict_hint: str = (
        "Pass a distinct `tool_prefix=...` to this toolset so its tool names and "
        "model-facing hints are renamed together; wrapping it in `PrefixedToolset` "
        "renames only the tools."
    )

    def _set_tool_prefix(self, tool_prefix: str) -> None:
        validate_tool_prefix(tool_prefix)
        self._tool_prefix = tool_prefix

    def _build_tool_name(self, base: str) -> str:
        return f"{self._tool_prefix}_{base}" if self._tool_prefix else base

    def _find_base_tool_name(self, name: str) -> str | None:
        """Return ``name`` without the prefix, or ``None`` when a prefix is set and ``name`` does not carry it."""
        if not self._tool_prefix:
            return name
        prefix = f"{self._tool_prefix}_"
        return name.removeprefix(prefix) if name.startswith(prefix) else None

    def _get_tool_prefix_id_suffix(self, separator: str = "-") -> str:
        return f"{separator}{self._tool_prefix}" if self._tool_prefix else ""
