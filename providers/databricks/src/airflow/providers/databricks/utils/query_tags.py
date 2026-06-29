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
#
"""Shared utilities for Databricks query-tag handling."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


def get_airflow_query_tags(context: Context) -> dict[str, str | None]:
    """Return Airflow context metadata as a query-tags dict."""
    task_instance = context.get("ti")
    if task_instance is None:
        return {}

    def _as_str(value: Any) -> str | None:
        return None if value is None else str(value)

    return {
        "airflow_dag_id": _as_str(task_instance.dag_id),
        "airflow_task_id": _as_str(task_instance.task_id),
        "airflow_run_id": _as_str(task_instance.run_id),
        "airflow_try_number": _as_str(task_instance.try_number),
        "airflow_map_index": _as_str(task_instance.map_index),
    }


def build_query_tags(
    context: Context | None,
    user_query_tags: dict[str, str | None],
    include_airflow_query_tags: bool,
) -> dict[str, str | None] | None:
    """
    Merge Airflow context tags with user-supplied tags.

    Airflow tags are added first; user-supplied tags override on key collision.
    Returns ``None`` when the resulting dict is empty so callers can skip
    injection entirely.
    """
    tags: dict[str, str | None] = {}
    if include_airflow_query_tags and context is not None:
        tags.update(get_airflow_query_tags(context))
    tags.update(user_query_tags)
    return tags or None


def dict_to_query_tag_list(tags: dict[str, str | None]) -> list[dict[str, str]]:
    """
    Convert a ``{key: value}`` dict to the ``[{"key": ..., "value": ...}]`` list format.

    Required by the Databricks Statement Execution REST API.

    See: https://docs.databricks.com/api/workspace/statementexecution/executestatement
    Entries whose value is ``None`` are omitted.
    """
    return [{"key": k, "value": v} for k, v in tags.items() if v is not None]
