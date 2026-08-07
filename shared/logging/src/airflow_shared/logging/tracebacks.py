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
"""Render :class:`structlog.tracebacks.ExceptionDictTransformer` output as text."""

from __future__ import annotations

from typing import Any

__all__ = ["format_exception_dicts"]

_CAUSE_MESSAGE = "The above exception was the direct cause of the following exception:"
_CONTEXT_MESSAGE = "During handling of the above exception, another exception occurred:"
_GROUP_HEADER = "Exception Group Traceback (most recent call last):"
_SEPARATOR_DASHES = 16
_CLOSING_DASHES = 36
# Groups can nest arbitrarily deep. CPython's own traceback module stops at 15 levels and so
# do we, both to keep the output readable and to bound recursion on a hostile payload.
_MAX_GROUP_DEPTH = 15


class _MalformedPayload(Exception):
    """Raised internally when the payload does not match the transformer's schema."""


def format_exception_dicts(exc_dicts: Any) -> str | None:
    """
    Render structlog's dict representation of an exception back into a traceback.

    The payload reaches us as JSON from a separate process, so anything at all may turn up
    here. Rather than risk taking down the log pipeline, an unusable payload renders as
    ``None`` and lets the caller keep whatever it already had.

    :param exc_dicts: the value produced by
        :class:`structlog.tracebacks.ExceptionDictTransformer`.
    :return: a multi-line string, or ``None`` if the payload could not be rendered.
    """
    if not isinstance(exc_dicts, list) or not exc_dicts:
        return None
    if not all(isinstance(entry, dict) and entry.get("exc_type") for entry in exc_dicts):
        return None
    try:
        return "\n".join(_format_chain(exc_dicts, depth=0))
    except Exception:
        # Losing the stack trace is annoying. Losing the log line it belongs to is worse.
        return None


def _format_chain(chain: list[Any], depth: int) -> list[str]:
    """Render one exception chain, innermost cause first, at the given group nesting depth."""
    lines: list[str] = []
    ordered = list(reversed(chain))
    for position, stack in enumerate(ordered):
        lines.extend(_format_stack(stack, depth))
        if position < len(ordered) - 1:
            banner = _CAUSE_MESSAGE if stack.get("is_cause") else _CONTEXT_MESSAGE
            lines += _prefixed(["", banner, ""], depth)
    return lines


def _prefixed(lines: list[str], depth: int) -> list[str]:
    """Apply the margin that marks lines as belonging to an enclosing exception group."""
    if not depth:
        return lines
    margin = "  " * depth + "| "
    return [margin + line for line in lines]


def _format_stack(stack: Any, depth: int) -> list[str]:
    """Render a single exception together with its stack frames."""
    if stack.get("is_group") and stack.get("exceptions"):
        return _format_group(stack, depth)

    lines: list[str] = []
    frames = _as_list(stack.get("frames"))
    if frames:
        lines.append("Traceback (most recent call last):")
        for frame in frames:
            lines.extend(_format_frame(frame))

    summary = stack.get("exc_value")
    if syntax_error := stack.get("syntax_error"):
        lines.extend(_format_syntax_error(syntax_error))
        # The file and line are already on the preceding lines, so prefer the bare message
        # over ``exc_value``, which repeats them.
        summary = syntax_error.get("msg", summary)

    lines.append(_exception_line(stack.get("exc_type"), summary))
    lines.extend(_as_list(stack.get("exc_notes")))
    return _prefixed(lines, depth)


def _exception_line(exc_type: Any, summary: Any) -> str:
    """Render the ``Type: message`` line, dropping the separator when there is no message."""
    # `raise ValueError()` carries an empty `exc_value`, and CPython prints the bare type name.
    return f"{exc_type}: {summary}" if summary else f"{exc_type}"


def _as_list(value: Any) -> list[Any]:
    """Return an empty or list value unchanged, and reject anything else."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise _MalformedPayload(type(value).__name__)
    return value


def _format_group(stack: Any, depth: int) -> list[str]:
    """Render an :exc:`ExceptionGroup` and its sub-exceptions as indented, numbered blocks."""
    # A group at the top level still draws its own margin, so it renders one level in.
    group_depth = depth or 1
    indent = "  " * group_depth
    child_indent = "  " * (group_depth + 1)

    header: list[str] = []
    frames = _as_list(stack.get("frames"))
    if frames:
        header.append(_GROUP_HEADER)
        for frame in frames:
            header.extend(_format_frame(frame))
    header.append(_exception_line(stack.get("exc_type"), stack.get("exc_value")))
    header.extend(_as_list(stack.get("exc_notes")))

    lines = _prefixed(header, group_depth)
    if frames and depth == 0:
        # CPython marks the outermost group header with a `+` rather than the usual `|`.
        lines[0] = f"{indent}+ {_GROUP_HEADER}"

    children = _as_list(stack.get("exceptions"))
    if group_depth >= _MAX_GROUP_DEPTH:
        return [*lines, f"{child_indent}+ ... ({len(children)} more nested sub-exceptions)"]

    for position, child in enumerate(children):
        divider = f"{'-' * _SEPARATOR_DASHES} {position + 1} {'-' * _SEPARATOR_DASHES}"
        lines.append(f"{indent}+-+{divider}" if position == 0 else f"{child_indent}+{divider}")
        lines.extend(_format_chain(_as_list(child), group_depth + 1))

    # The last sub-exception draws the closing line itself when it is a group of its own.
    if not _ends_in_group(children):
        lines.append(f"{child_indent}+{'-' * _CLOSING_DASHES}")
    return lines


def _ends_in_group(children: list[Any]) -> bool:
    """Tell whether the last sub-exception rendered is itself an exception group."""
    if not children or not isinstance(children[-1], list) or not children[-1]:
        return False
    last = children[-1][0]
    return bool(isinstance(last, dict) and last.get("is_group") and last.get("exceptions"))


def _format_syntax_error(syntax_error: Any) -> list[str]:
    """Render the offending source line of a :exc:`SyntaxError` with a caret beneath it."""
    lines = [f'  File "{syntax_error.get("filename")}", line {syntax_error.get("lineno")}']
    source = (syntax_error.get("line") or "").rstrip("\n")
    if not source.strip():
        return lines

    # Match CPython, which strips only spaces, newlines and form feeds so that tab-indented
    # source keeps its tabs, and pads the caret with the whitespace it kept so it lines up.
    stripped = source.lstrip(" \n\f")
    lines.append(f"    {stripped}")
    offset = syntax_error.get("offset")
    if isinstance(offset, int):
        column = offset - 1 - (len(source) - len(stripped))
        if 0 <= column < len(stripped):
            padding = "".join(char if char.isspace() else " " for char in stripped[:column])
            lines.append(f"    {padding}^")
    return lines


def _format_frame(frame: Any) -> list[str]:
    """Render one stack frame, including the source line when structlog captured it."""
    filename = frame.get("filename")
    lineno = frame.get("lineno")
    name = frame.get("name")
    # structlog replaces the middle of an over-long traceback with a placeholder frame that
    # carries only a count, so rendering it as a `File "", line -1` entry would be nonsense.
    if not filename and lineno == -1:
        return [f"  [{name}]"]
    lines = [f'  File "{filename}", line {lineno}, in {name}']
    if source := frame.get("line"):
        lines.append(f"    {source.strip()}")
    return lines
