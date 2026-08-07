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
Bounded, columnar payloads for the ``query`` tool of the SQL toolsets.

A tool result stays in the model's message history for the rest of the run, so its
cost is re-paid on every subsequent request. Two things here keep that bounded:

* **Columnar shape.** ``{"columns": [...], "rows": [[...], ...]}`` names each column
  once instead of repeating it in a dict per row. On a table with thousands of
  columns the repeated names, not the values, are the bulk of the payload.
* **A byte budget.** ``max_rows`` caps rows, which says nothing about size -- a
  single row of a 3000-column table dwarfs a thousand rows of a narrow one. The
  budget here is what actually bounds context, and when it bites the payload says
  so, in terms the agent can act on (narrow the projection).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

# A policy default, not a limit imposed by any storage, protocol, or model layer:
# roughly 16k tokens at 4 characters per token. Large enough that ordinary queries are
# unaffected, small enough that no single tool result can dominate the context window.
# Deployments that keep many results in history should lower it.
DEFAULT_MAX_RESULT_BYTES = 65_536

# Tool results are machine-read, so no whitespace. ensure_ascii=False matters as much as
# the separators: escaping one CJK character to \uXXXX costs six bytes instead of three,
# so an ASCII-escaped result is charged several times over against the budget and
# truncated that much earlier than an equivalent English one.
_DUMP_KWARGS: dict[str, Any] = {"default": str, "separators": (",", ":"), "ensure_ascii": False}

#: Description for the ``query`` tool. States the columnar shape, since the model has
#: to align each row's values to ``columns`` positionally, and the truncation contract,
#: so a short result is not read as an empty table.
QUERY_TOOL_DESCRIPTION = (
    "Execute a SQL query. Returns JSON of the form "
    '{"columns": [name, ...], "rows": [[value, ...], ...]}, where each row holds its '
    "values in column order. A `truncated` key means the query matched more than was "
    "returned and `truncated_by` names the limit that was hit; narrow the projection or "
    "aggregate in SQL rather than paging through the result."
)


def _dumps(payload: Any) -> str:
    return json.dumps(payload, **_DUMP_KWARGS)


def _size(payload: Any) -> int:
    """Measure the serialized size in bytes -- not characters, which diverge outside ASCII."""
    return len(_dumps(payload).encode("utf-8"))


def build_query_result(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    max_rows: int,
    max_result_bytes: int,
    more_rows_available: bool,
    total_rows: int | None = None,
) -> str:
    """
    Render query rows as a bounded, columnar JSON tool result.

    :param columns: Column names, in the order the values appear in each row.
    :param rows: Rows already capped to ``max_rows``; only the byte budget is
        applied here.
    :param max_rows: The row cap that produced *rows*, reported back to the agent
        so it knows which limit it hit.
    :param max_result_bytes: Budget for the serialized column names plus rows.
        The surrounding envelope (the ``truncated``/``hint`` keys) adds a small
        fixed amount on top.
    :param more_rows_available: Whether the query matched more rows than *rows*
        holds.
    :param total_rows: Total rows the driver reported for the query, when it
        reports one at all. ``None`` is common and is not an error -- SQLite and
        several warehouse drivers do not populate it for ``SELECT``.
    """
    budget = max_result_bytes - _size(list(columns))
    if budget < 0:
        # The column names alone blow the budget, so returning them would spend the
        # whole context on a header and leave no room for data. Report the shape and
        # tell the agent to narrow the projection -- the only move that helps here.
        output: dict[str, Any] = {
            "column_count": len(columns),
            "rows": [],
            "row_count": 0,
            "truncated": True,
            "truncated_by": "max_result_bytes",
            "hint": (
                f"This query returns {len(columns)} column{'' if len(columns) == 1 else 's'}, "
                f"whose names alone exceed max_result_bytes ({max_result_bytes}). Select the "
                f"specific columns you need instead of all of them."
            ),
        }
        if total_rows is not None:
            output["total_rows"] = total_rows
        return _dumps(output)

    # Rows are kept as a contiguous prefix: stopping at the first row that does not fit
    # rather than skipping it and packing later ones, so "rows 1..n" means what it says
    # and the agent is never handed a result with a hole in the middle.
    kept: list[list[Any]] = []
    for row in rows:
        as_list = list(row)
        # +1 for the comma joining this row to the previous one; the serializer and
        # separators match the final dump, so this accounting is exact.
        cost = _size(as_list) + (1 if kept else 0)
        if cost > budget:
            break
        budget -= cost
        kept.append(as_list)

    byte_capped = len(kept) < len(rows)
    output = {"columns": list(columns), "rows": kept, "row_count": len(kept)}
    if total_rows is not None:
        output["total_rows"] = total_rows

    if byte_capped or more_rows_available:
        output["truncated"] = True
        output["truncated_by"] = "max_result_bytes" if byte_capped else "max_rows"
    if byte_capped:
        # Say which row stopped it. "No row fits" would be false whenever a single wide
        # row sits in front of narrow ones, and a partial result with no guidance at all
        # is the common case, not the empty one.
        output["hint"] = (
            f"The first row alone exceeds max_result_bytes ({max_result_bytes}). "
            if not kept
            else f"Stopped after {len(kept)} row{'' if len(kept) == 1 else 's'}: the next row "
            f"did not fit in max_result_bytes ({max_result_bytes}). "
        ) + "Select fewer columns, or aggregate, instead of returning whole rows."
    elif more_rows_available:
        output["hint"] = (
            f"Only the first {max_rows} rows are shown. Filter or aggregate in SQL rather "
            f"than paging through the result."
        )
    return _dumps(output)
