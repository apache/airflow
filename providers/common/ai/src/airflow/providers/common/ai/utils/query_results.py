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
Bounded payloads for the ``query`` and ``get_schema`` tools of the SQL toolsets.

A tool result stays in the model's message history for the rest of the run, so its
cost is re-paid on every subsequent request. Three things here keep that bounded:

* **Columnar query results.** ``{"columns": [...], "rows": [[...], ...]}`` names each
  column once instead of repeating it in a dict per row. On a table with thousands of
  columns the repeated names, not the values, are the bulk of the payload.
* **A byte budget.** ``max_rows`` and ``max_columns`` cap how many rows or columns come
  back, which says nothing about size -- a single row of a 3000-column table dwarfs a
  thousand rows of a narrow one. The budget here is what actually bounds context, and
  when it bites the payload says so, in terms the agent can act on (narrow the
  projection, or filter the columns).
* **A column cap for get_schema.** Column names are what the agent needs to write SQL,
  so above ``max_columns`` the full list is replaced by a summary (count, a type
  histogram, a sample) that points at the ``name_contains`` filter rather than
  truncating blindly.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

from airflow.providers.common.ai.utils.masking import dumps_masked

# A policy default, not a limit imposed by any storage, protocol, or model layer:
# roughly 16k tokens at 4 characters per token. Large enough that ordinary queries are
# unaffected, small enough that no single tool result can dominate the context window.
# Deployments that keep many results in history should lower it.
DEFAULT_MAX_RESULT_BYTES = 65_536

# Column cap for ``get_schema`` above which the full column list is replaced by a summary. The
# byte budget is the real backstop. This is a deterministic, count-based trigger that is easy for
# the agent to reason about and keeps an ordinary-width table returning in full.
DEFAULT_MAX_COLUMNS = 100

# Cap on distinct keys in a ``get_schema`` type histogram. Parametrized types (``VARCHAR(255)``,
# ``Decimal128(38,10)``) would otherwise produce a distinct key per parameterization and defeat the
# bound, so everything past the most common ``_SCHEMA_TYPE_HISTOGRAM_TOP_K`` is folded into one entry.
_SCHEMA_TYPE_HISTOGRAM_TOP_K = 20
_SCHEMA_TYPE_HISTOGRAM_OTHER_KEY = "(other)"

# Columns previewed in a ``get_schema`` summary. A small fixed sample -- deliberately not the
# first ``max_columns`` -- keeps the summary much smaller than the list it replaces and reads as
# "here is what the names look like, now filter", never as a usable prefix of the table.
_SCHEMA_SAMPLE_SIZE = 20

# Tool results are machine-read, so no whitespace. ensure_ascii=False matters as much as
# the separators: escaping one CJK character to \uXXXX costs six bytes instead of three,
# so an ASCII-escaped result is charged several times over against the budget and
# truncated that much earlier than an equivalent English one.
_DUMP_KWARGS: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False}

#: Description for the ``query`` tool. States the columnar shape, since the model has
#: to align each row's values to ``columns`` positionally, and the truncation contract,
#: so a short result is not read as an empty table.
QUERY_TOOL_DESCRIPTION = (
    "Execute a SQL query. Returns JSON of the form "
    '{"columns": [name, ...], "rows": [[value, ...], ...]}, where each row holds its '
    "values in column order. A `truncated` key means you are not seeing the whole result "
    "-- either more rows matched than were returned, or the result was too large -- and "
    "`truncated_by` names the limit that was hit; narrow the projection or aggregate in "
    "SQL rather than paging through the result."
)

#: Description for the ``get_schema`` tool. Column names are what the agent needs to write SQL, so
#: unlike query rows they cannot be silently dropped: the description states the ``name_contains``
#: filter and the summary shape so a truncated result is read as "narrow your request", not as a
#: small table.
GET_SCHEMA_TOOL_DESCRIPTION = (
    "Get a table's columns. Returns JSON of the form "
    '{"columns": [{"name": ..., "type": ...}, ...], "column_count": N}. Pass `name_contains` to '
    "return only the columns whose name contains that substring (case-insensitive) -- use it to "
    "find the columns relevant to your question on a wide table. When a table has more columns "
    "than can be returned at once, the full list is replaced by a summary (`column_count`, and "
    "where the budget allows a `type_histogram` and a `sample_columns` preview) with `truncated` "
    "set, `truncated_by` naming the limit that was hit, and a `hint`. Call get_schema again with "
    "a `name_contains` substring to retrieve the specific columns you need instead of the whole "
    "table."
)


def _dumps(payload: Any) -> str:
    return dumps_masked(payload, **_DUMP_KWARGS)


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


def _build_type_histogram(columns: Sequence[dict[str, str]]) -> dict[str, int]:
    """
    Count columns per type, capped to the most common ``_SCHEMA_TYPE_HISTOGRAM_TOP_K`` types.

    Ordered by ``(-count, type)`` so the output is content-stable (deterministic for byte
    accounting and tests, not merely input-ordered). The long tail is folded into a single
    aggregate entry rather than listed, so parametrized types cannot inflate the key count.

    :param columns: ``{"name", "type"}`` dicts.
    """
    counts = Counter(col["type"] for col in columns)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    if len(ordered) <= _SCHEMA_TYPE_HISTOGRAM_TOP_K:
        return dict(ordered)
    histogram = dict(ordered[:_SCHEMA_TYPE_HISTOGRAM_TOP_K])
    histogram[_SCHEMA_TYPE_HISTOGRAM_OTHER_KEY] = sum(
        count for _, count in ordered[_SCHEMA_TYPE_HISTOGRAM_TOP_K:]
    )
    return histogram


def build_schema_result(
    columns: Sequence[dict[str, str]],
    *,
    max_columns: int,
    max_result_bytes: int,
    name_contains: str | None = None,
) -> str:
    """
    Render a table's columns as a bounded JSON tool result.

    Column names are the information an agent needs to write SQL, so unlike query rows they cannot
    simply be dropped: above ``max_columns`` (or the byte budget) the full list is replaced by a
    summary -- count, a type histogram, and a sample of columns -- that names ``name_contains`` as
    the way to retrieve specific columns. ``columns`` is assumed to hold distinct names (a table's
    introspected columns are unique by construction).

    :param columns: ``{"name", "type"}`` dicts in table order.
    :param max_columns: Column count above which a summary replaces the full list.
    :param max_result_bytes: Budget for the serialized result.
    :param name_contains: Case-insensitive substring. When given (and non-empty), only matching
        columns are considered and the value is echoed back so a filtered subset is never mistaken
        for the whole table.
    """
    name_contains = name_contains or None
    total_columns = len(columns)
    if name_contains is not None:
        needle = name_contains.casefold()
        selected = [col for col in columns if needle in col["name"].casefold()]
    else:
        selected = list(columns)

    if name_contains is not None and not selected:
        plural = "" if total_columns == 1 else "s"
        # Do not tell the agent to call without name_contains when the full list would itself be
        # summarized (total_columns > max_columns) -- that would bounce it back to this filter.
        if total_columns > max_columns:
            advice = (
                f"No columns match name_contains={name_contains!r}. Try a different substring "
                f"(the table has {total_columns} column{plural}, too many to list in full)."
            )
        else:
            advice = (
                f"No columns match name_contains={name_contains!r}. Call get_schema without "
                f"name_contains to list all {total_columns} column{plural}."
            )
        return _dumps(
            {
                "columns": [],
                "column_count": 0,
                "name_contains": name_contains,
                "total_columns": total_columns,
                "hint": advice,
            }
        )

    full: dict[str, Any] = {"columns": selected, "column_count": len(selected)}
    if name_contains is not None:
        full["name_contains"] = name_contains
        full["total_columns"] = total_columns
    if len(selected) <= max_columns and _size(full) <= max_result_bytes:
        return _dumps(full)

    return _summarize_schema(
        selected,
        total_columns=total_columns,
        max_columns=max_columns,
        max_result_bytes=max_result_bytes,
        name_contains=name_contains,
    )


def _summarize_schema(
    selected: list[dict[str, str]],
    *,
    total_columns: int,
    max_columns: int,
    max_result_bytes: int,
    name_contains: str | None,
) -> str:
    """Build the bounded summary returned when the full column list does not fit."""
    # Count is checked before bytes: it is the cheaper, more explainable bound. A bytes-only
    # truncation then means "count fits but names/types are pathologically long", a rarer signal.
    n = len(selected)
    plural = "" if n == 1 else "s"
    truncated_by = "max_columns" if n > max_columns else "max_result_bytes"
    subject = f"name_contains={name_contains!r} matched" if name_contains is not None else "This table has"
    if truncated_by == "max_columns":
        reason = f"{subject} {n} column{plural}, more than the max_columns limit of {max_columns}."
    else:
        reason = f"{subject} {n} column{plural} that did not fit max_result_bytes ({max_result_bytes})."
    move = (
        "Use a more specific name_contains substring to narrow to the columns you need."
        if name_contains is not None
        else "Call get_schema with a name_contains substring to return only the columns you need."
    )

    output: dict[str, Any] = {
        "column_count": n,
        "truncated": True,
        "truncated_by": truncated_by,
        "hint": f"{reason} {move}",
    }
    if name_contains is not None:
        output["name_contains"] = name_contains
        output["total_columns"] = total_columns

    # The core above is the guaranteed-useful payload. Add the histogram and a small sample of
    # columns only while they fit, reserving the core first. If the histogram alone would not fit,
    # drop it but still fill the sample from what remains, so a wide struct type cannot strip the
    # preview off every other column. A pathologically small budget still returns the core.
    output["type_histogram"] = _build_type_histogram(selected)
    output["sample_columns"] = []
    if _size(output) > max_result_bytes:
        del output["type_histogram"]
    budget = max_result_bytes - _size(output)
    for col in selected[:_SCHEMA_SAMPLE_SIZE]:
        cost = _size(col) + (1 if output["sample_columns"] else 0)
        if cost > budget:
            break
        budget -= cost
        output["sample_columns"].append(col)
    return _dumps(output)
