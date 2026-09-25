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
Cross-attempt ``pydantic_ai.usage.RunUsage`` accounting, backed by the task state store.

``RunUsage`` is a plain (non-pydantic) dataclass, so it needs manual JSON
(de)serialization -- ``dump_run_usage`` / ``load_run_usage`` do that, iterating
``dataclasses.fields`` rather than hard-coding the field list so a future
pydantic-ai field is carried through automatically. This module has no
top-level import of any Airflow >= 3.3-only symbol: it must stay importable on
older cores, even though :class:`TaskStateStoreUsageBudget` is only
constructed on 3.3+ (see ``AgentOperator._build_usage_budget``).
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import structlog
from pydantic_ai.usage import RunUsage

if TYPE_CHECKING:
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor

log = structlog.get_logger(logger_name="task")

# Reserved task state store key for the cumulative cross-attempt usage record. Separate
# from durable's ``DURABLE_KEY_PREFIX`` (see durable/base.py) so it is never mistaken for
# a durable replay step. No ``/`` -- a task state store key is a single, un-encoded URL
# path segment.
USAGE_BUDGET_KEY = "__commonai_usage__"

_RUN_USAGE_FIELDS = dataclasses.fields(RunUsage)


def dump_run_usage(usage: RunUsage) -> dict[str, Any]:
    """Serialize a ``RunUsage`` to a JSON-safe dict."""
    data: dict[str, Any] = {}
    for field in _RUN_USAGE_FIELDS:
        value = getattr(usage, field.name)
        if field.name == "cost":
            # Decimal | None, stringified so JSON round-trips it losslessly (mirrors
            # the XCom usage payload, see utils/logging.py:format_usage_for_xcom).
            data[field.name] = str(value) if value is not None else None
        else:
            data[field.name] = value
    return data


def load_run_usage(raw: Any, *, key: str) -> RunUsage:
    """
    Deserialize a dict produced by :func:`dump_run_usage` back into a ``RunUsage``.

    Unknown keys in *raw* are ignored, so a record written by a newer version of this
    module still loads. Only the fields ``RunUsage`` currently declares are read.

    :raises ValueError: *raw* is not a dict, or a field has the wrong shape (``cost``
        not a valid number, a count field not an int, ``details`` not a dict). The
        message names *key* so the error points at which task state store key to
        delete to reset the budget.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"{key!r} in the task state store is not a dict (got {raw!r}); delete the key to reset."
        )
    kwargs: dict[str, Any] = {}
    for field in _RUN_USAGE_FIELDS:
        if field.name not in raw:
            continue
        value = raw[field.name]
        if field.name == "cost":
            if value is None:
                kwargs["cost"] = None
                continue
            try:
                kwargs["cost"] = value if isinstance(value, Decimal) else Decimal(str(value))
            except InvalidOperation:
                raise ValueError(
                    f"{key!r}['cost'] in the task state store is not a valid number (got {value!r}); "
                    "delete the key to reset."
                ) from None
        elif field.name == "details":
            if not isinstance(value, dict):
                raise ValueError(
                    f"{key!r}['details'] in the task state store is not a dict (got {value!r}); "
                    "delete the key to reset."
                )
            # Copied, not aliased: RunUsage.incr() mutates `details` in place (usage.py),
            # so handing back the caller's own dict would let a later incr() on the
            # loaded RunUsage silently mutate the raw dict this was read from (matters
            # most for copy_run_usage's dump/load round trip of a live RunUsage).
            kwargs["details"] = dict(value)
        else:
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(
                    f"{key!r}[{field.name!r}] in the task state store is not an int (got {value!r}); "
                    "delete the key to reset."
                )
            kwargs[field.name] = value
    return RunUsage(**kwargs)


def copy_run_usage(usage: RunUsage) -> RunUsage:
    """
    Return an independent copy of *usage*.

    ``copy.copy`` shares the ``details`` dict, which ``RunUsage.incr`` mutates in
    place (``usage.py`` ``_incr_usage_tokens``), so a shallow copy would let a later
    increment of the original leak into the copy. Round-tripping through
    :func:`dump_run_usage` / :func:`load_run_usage` copies ``details`` too.
    """
    return load_run_usage(dump_run_usage(usage), key="run_usage")


def subtract_run_usage(total: RunUsage, base: RunUsage) -> RunUsage:
    """
    Return the field-by-field usage in *total* that is not already in *base*.

    Unlike ``RunUsage.__sub__`` (``usage.py``), which returns ``None`` for ``cost``
    whenever it is unchanged -- indistinguishable from "unknown" -- ``cost`` here is
    ``None`` only when both sides are ``None``; otherwise it is a numeric delta
    (``0`` when unchanged), because the result is meant to be reported (XCom, logs),
    where a numeric zero and an unknown cost are not the same thing.
    """
    kwargs: dict[str, Any] = {}
    for field in _RUN_USAGE_FIELDS:
        if field.name == "details":
            kwargs["details"] = {
                name: total.details.get(name, 0) - base.details.get(name, 0)
                for name in total.details.keys() | base.details
            }
        elif field.name == "cost":
            kwargs["cost"] = (
                None if total.cost is None and base.cost is None else (total.cost or 0) - (base.cost or 0)
            )
        else:
            kwargs[field.name] = getattr(total, field.name) - getattr(base, field.name)
    return RunUsage(**kwargs)


class TaskStateStoreUsageBudget:
    """
    Persists cumulative ``RunUsage`` across task attempts in the AIP-103 task state store.

    The stored record also carries the task instance's ``max_tries`` at write time.
    Airflow bumps ``ti.max_tries`` when a task is cleared (``clear_task_instances``,
    ``airflow-core/src/airflow/models/taskinstance.py``) but never changes it on an
    ordinary retry (``handle_failure`` does not touch it). So a ``max_tries`` that
    differs from the current task instance's means this row was written before the
    most recent clear -- a new budget cycle -- and :meth:`load` starts over from zero
    instead of carrying a stale spend forward. A same-cycle retry, which never
    changes ``max_tries``, keeps accumulating.

    :param accessor: The task state store accessor for the current task instance
        (``context["task_state_store"]``).
    :param max_tries: The current task instance's ``max_tries``.
    """

    def __init__(self, accessor: TaskStateStoreAccessor, *, max_tries: int) -> None:
        self._store = accessor
        self._max_tries = max_tries

    def load(self) -> RunUsage:
        """Return the cumulative usage so far, or a fresh ``RunUsage()`` if none or stale."""
        raw = self._store.get(USAGE_BUDGET_KEY)
        if raw is None:
            return RunUsage()
        if not isinstance(raw, dict) or "usage" not in raw:
            raise ValueError(
                f"{USAGE_BUDGET_KEY!r} in the task state store is not a valid usage budget record "
                f"(got {raw!r}); delete the key in the Task State Store UI to reset the budget."
            )
        if raw.get("max_tries") != self._max_tries:
            return RunUsage()
        return load_run_usage(raw["usage"], key=USAGE_BUDGET_KEY)

    def save(self, usage: RunUsage) -> None:
        """Best-effort write; a failure here must never fail the task, only the caller's raise matters."""
        try:
            # NEVER_EXPIRE does not exist on cores before 3.3; imported lazily here so this
            # module keeps importing cleanly on older cores (this module's docstring).
            from airflow.sdk.execution_time.context import NEVER_EXPIRE

            record: dict[str, Any] = {
                "version": 1,
                "max_tries": self._max_tries,
                "usage": dump_run_usage(usage),
            }
            self._store.set(USAGE_BUDGET_KEY, record, retention=NEVER_EXPIRE)
        except Exception:
            log.warning("Usage budget: failed to persist cumulative usage", exc_info=True)

    def clear(self) -> None:
        """Best-effort delete, called once the whole execute succeeds."""
        try:
            self._store.delete(USAGE_BUDGET_KEY)
        except Exception:
            log.warning("Usage budget: failed to delete cumulative usage key", exc_info=True)
