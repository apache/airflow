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
Bounding helpers for sandbox output that the model reads.

Two independent caps apply everywhere: a line count and a byte budget,
whichever is reached first. Truncation never emits a partial line, so the model
never has to guess whether it is looking at a whole record.

The direction differs by tool, and that difference is the point:

- Command output keeps the **tail**. A traceback, a failing assertion and the
  exit status all live at the end; the head is usually build noise.
- File reads keep the **head** and report the next ``offset``, so the model can
  page forward through a file it is reading top-down.
"""

from __future__ import annotations

_UNITS = ((1024 * 1024, "MB"), (1024, "KB"))


def format_size(num_bytes: int) -> str:
    """Render a byte count the way the truncation notes show it to the model."""
    for scale, suffix in _UNITS:
        if num_bytes >= scale:
            return f"{num_bytes / scale:.1f}{suffix}"
    return f"{num_bytes}B"


def _tail_bytes(line: str, max_bytes: int) -> str:
    """Return the last ``max_bytes`` UTF-8 bytes of ``line``, dropping a partial leading char."""
    return line.encode("utf-8")[-max_bytes:].decode("utf-8", errors="ignore")


def _keep_lines(
    lines: list[str], *, max_lines: int, max_bytes: int, from_tail: bool
) -> tuple[list[str], bool]:
    """
    Keep whole lines that fit under both caps, returning ``(kept, truncated)``.

    Reverse up front when taking the tail so the accumulation loop always works
    on "the line we would keep first", then reverse back at the end.
    """
    ordered = lines[::-1] if from_tail else list(lines)

    # A single line wider than the whole byte budget cannot be kept intact. For
    # command output the end of that line is the useful part (a long final log
    # record, a big JSON error body), so keep a byte-suffix of it. For a file
    # read there is no useful prefix to show, so drop it and let the caller tell
    # the model to slice the file with a shell command instead.
    if ordered and len(ordered[0].encode("utf-8")) > max_bytes:
        if from_tail:
            return [_tail_bytes(ordered[0], max_bytes)], True
        return [], True

    kept: list[str] = []
    used = 0
    truncated = False
    for line in ordered:
        if len(kept) >= max_lines:
            truncated = True
            break
        # +1 for the newline that join() inserts before every line but the first.
        cost = len(line.encode("utf-8")) + (1 if kept else 0)
        if used + cost > max_bytes:
            truncated = True
            break
        kept.append(line)
        used += cost

    if from_tail:
        kept.reverse()
    return kept, truncated


def truncate_output(
    text: str,
    *,
    max_lines: int,
    max_bytes: int,
    already_truncated: bool = False,
) -> str:
    """
    Cap free-form command output, keeping the tail and marking anything dropped.

    ``already_truncated`` says the backend dropped bytes while reading the
    stream, before this function saw ``text``. The cut is then marked even when
    what survives happens to fit, so the model is never told it has the whole
    output when it does not.
    """
    if not text:
        return text
    kept, truncated = _keep_lines(text.splitlines(), max_lines=max_lines, max_bytes=max_bytes, from_tail=True)
    body = "\n".join(kept)
    if truncated or already_truncated:
        return f"[... earlier output truncated]\n{body}"
    return body


def render_file_window(
    data: bytes,
    *,
    offset: int | None,
    limit: int | None,
    max_lines: int,
    max_bytes: int,
) -> str:
    """
    Render a window of a text file, head-first, with a continuation offset.

    ``offset`` is a 1-indexed line number and ``limit`` a line count, both as the
    model supplies them. The safety caps apply on top, so a file with very long
    lines is bounded even when the model asks for few lines.
    """
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    total = len(lines)

    start = max(1, offset or 1)
    if start > total:
        return f"(no lines at offset {start}; file has {total} line{'s' if total != 1 else ''})"
    window = lines[start - 1 :]
    if limit is not None and limit > 0:
        window = window[:limit]

    kept, truncated = _keep_lines(window, max_lines=max_lines, max_bytes=max_bytes, from_tail=False)
    if not kept and truncated:
        return (
            f"Line {start} is longer than the {format_size(max_bytes)} output limit. "
            "Read part of it with a shell command instead (e.g. cut, head -c, or sed)."
        )

    body = "\n".join(kept)
    next_offset = start + len(kept)
    if truncated or next_offset <= total:
        remaining = total - (next_offset - 1)
        return (
            f"{body}\n[... {remaining} more line{'s' if remaining != 1 else ''}; "
            f"read on with offset={next_offset}]"
        )
    return body
