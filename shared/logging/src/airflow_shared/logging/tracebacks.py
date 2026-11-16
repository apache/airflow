#
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
from __future__ import annotations

from typing import Any

_CAUSE = "The above exception was the direct cause of the following exception:"
_CONTEXT = "During handling of the above exception, another exception occurred:"


def format_error_detail(error_detail: Any) -> str | None:
    """
    Render the exception dicts from structlog's ``ExceptionDictTransformer`` as a traceback.

    structlog lists the raised exception first and its cause or context after it, which is the
    reverse of the order Python prints a chain in, so the stacks are rendered from the end.
    Returns ``None`` instead of raising when ``error_detail`` is empty or rendering it fails, so a
    caller in a log loop can fall back to the raw value.
    """
    try:
        return "\n".join(_format_stacks(error_detail)) or None
    except Exception:
        return None


def _format_stacks(stacks: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for i, stack in enumerate(reversed(stacks)):
        if i:
            # is_cause on a stack means it is the direct cause of the stack listed before it.
            lines += ["", _CAUSE if stacks[-i].get("is_cause") else _CONTEXT, ""]
        is_group = stack.get("is_group", False)
        if stack.get("frames"):
            lines.append(f"{'Exception Group ' if is_group else ''}Traceback (most recent call last):")
        for frame in stack.get("frames", []):
            if frame.get("lineno") == -1:
                lines.append(f"  [{frame.get('name', '')}]")
            else:
                filename, lineno = frame.get("filename", "<unknown>"), frame.get("lineno", "?")
                lines.append(f'  File "{filename}", line {lineno}, in {frame.get("name", "<unknown>")}')
        exc_type, exc_value = stack.get("exc_type", ""), stack.get("exc_value", "")
        lines.append(f"{exc_type}: {exc_value}" if exc_value else exc_type)
        lines.extend(stack.get("exc_notes", []))
        for n, sub_stacks in enumerate(stack.get("exceptions", []), 1):
            lines.append(f"  +---- {n} ----")
            sub_lines = "\n".join(_format_stacks(sub_stacks)).splitlines()
            lines.extend(f"  | {line}" for line in sub_lines)
    return lines
