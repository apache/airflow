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
"""FastMCP tool surface for the Airflow internal development MCP server.

All tools are thin, read-mostly wrappers over :func:`airflow_mcp_server.client.call_api`,
which owns authentication and the method safety gate. Write tools are always
registered (so the tool list is stable across restarts); calling one without
``AIRFLOW_MCP_ALLOW_WRITES`` set raises a ``ToolError`` that explains how to enable writes.
``DELETE`` is gated more strictly still, behind its own ``AIRFLOW_MCP_ALLOW_DELETES`` flag.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from airflow_mcp_server.client import call_api

mcp = FastMCP(
    "airflow-dev",
    instructions=(
        "Tools for inspecting and debugging a single running Airflow instance (typically a "
        "local Breeze development environment) through its public REST API. Read-only by "
        "default; write tools require the AIRFLOW_MCP_ALLOW_WRITES environment variable, and "
        "deletions require the stricter, separate AIRFLOW_MCP_ALLOW_DELETES flag. This is an "
        "internal development tool, not part of any Airflow release."
    ),
)

_DAG_SUMMARY_FIELDS = (
    "dag_id",
    "is_paused",
    "is_stale",
    "has_import_errors",
    "next_dagrun_run_after",
    "timetable_summary",
    "owners",
)
_TASK_INSTANCE_SUMMARY_FIELDS = (
    "task_id",
    "state",
    "try_number",
    "start_date",
    "end_date",
    "duration",
    "operator",
    "map_index",
)
_CONNECTION_SUMMARY_FIELDS = ("connection_id", "conn_type", "host", "schema", "port", "login", "description")
_VARIABLE_SUMMARY_FIELDS = ("key", "is_encrypted", "description")
_FAILED_TASK_STATES = {"failed", "upstream_failed"}


def _pick(item: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: item[field] for field in fields if field in item}


def _clean_params(**kwargs: Any) -> dict[str, Any]:
    """Drop ``None`` values so they are omitted from the query string entirely.

    httpx encodes a ``None`` query param value as an empty string rather than
    dropping it, which would send e.g. ``paused=`` instead of leaving ``paused`` unset.
    """
    return {key: value for key, value in kwargs.items() if value is not None}


def _summarize_dag(dag: dict[str, Any]) -> dict[str, Any]:
    summary = _pick(dag, _DAG_SUMMARY_FIELDS)
    summary["tags"] = [tag["name"] for tag in dag.get("tags", [])]
    return summary


@mcp.tool(annotations={"readOnlyHint": True})
async def get_version() -> Any:
    """Get the Airflow version and git version of the running instance.

    Use this as a health check: if it fails, the Airflow API server is unreachable or down.
    """
    return await call_api("GET", "/api/v2/version")


@mcp.tool(annotations={"readOnlyHint": True})
async def list_dags(
    limit: Annotated[int, Field(description="Max number of Dags to return.")] = 50,
    offset: Annotated[int, Field(description="Number of Dags to skip, for pagination.")] = 0,
    dag_id_pattern: Annotated[
        str | None, Field(description="SQL LIKE pattern (e.g. '%customer%') to filter by dag_id.")
    ] = None,
    paused: Annotated[bool | None, Field(description="Filter by paused state; omit for both.")] = None,
) -> Any:
    """List Dags known to the scheduler, trimmed to the fields most useful for orientation.

    Each item includes dag_id, is_paused, is_stale, has_import_errors,
    next_dagrun_run_after, timetable_summary, owners, and tags. Use `get_dag` for full detail
    on a single Dag.
    """
    params = _clean_params(limit=limit, offset=offset, dag_id_pattern=dag_id_pattern, paused=paused)
    result = await call_api("GET", "/api/v2/dags", params=params)
    return {
        "dags": [_summarize_dag(dag) for dag in result.get("dags", [])],
        "total_entries": result.get("total_entries"),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_dag(dag_id: str) -> Any:
    """Get full details of a single Dag: schedule, params, tags, owners, doc, and more."""
    return await call_api("GET", f"/api/v2/dags/{dag_id}/details")


@mcp.tool(annotations={"readOnlyHint": True})
async def get_dag_source(
    dag_id: str,
    version_number: Annotated[
        int | None, Field(description="Specific Dag version to fetch source for; omit for the latest.")
    ] = None,
) -> Any:
    """Get the parsed source code of a Dag file."""
    params = _clean_params(version_number=version_number)
    return await call_api("GET", f"/api/v2/dagSources/{dag_id}", params=params)


@mcp.tool(annotations={"readOnlyHint": True})
async def list_dag_runs(
    dag_id: Annotated[str, Field(description="Dag id, or '~' to list runs across all Dags.")],
    limit: int = 20,
    offset: int = 0,
    state: Annotated[str | None, Field(description="Filter by run state, e.g. 'failed'.")] = None,
    order_by: Annotated[str, Field(description="Sort field, prefix with '-' for descending.")] = "-run_after",
) -> Any:
    """List Dag runs for a Dag (or all Dags, with dag_id='~'), newest first by default."""
    params = _clean_params(limit=limit, offset=offset, order_by=[order_by])
    if state:
        params["state"] = [state]
    return await call_api("GET", f"/api/v2/dags/{dag_id}/dagRuns", params=params)


@mcp.tool(annotations={"readOnlyHint": True})
async def list_task_instances(
    dag_id: str,
    dag_run_id: str,
    state: Annotated[str | None, Field(description="Filter by task instance state.")] = None,
    limit: int = 100,
) -> Any:
    """List task instances of a Dag run, trimmed to task_id, state, try_number, timing, and operator."""
    params = _clean_params(limit=limit)
    if state:
        params["state"] = [state]
    result = await call_api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params=params)
    return {
        "task_instances": [
            _pick(ti, _TASK_INSTANCE_SUMMARY_FIELDS) for ti in result.get("task_instances", [])
        ],
        "total_entries": result.get("total_entries"),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: Annotated[int, Field(description="Mapped task index; -1 for an unmapped task.")] = -1,
) -> Any:
    """Get full detail of a single task instance (mapped or unmapped)."""
    if map_index >= 0:
        path = f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}"
    else:
        path = f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
    return await call_api("GET", path)


def _format_error_detail(error_detail: list[Any]) -> list[str]:
    """Render the structured exception chain of a log event as traceback-like lines."""
    lines = []
    for exc in error_detail:
        if not isinstance(exc, dict):
            lines.append(str(exc))
            continue
        for frame in exc.get("frames") or []:
            lines.append(
                f'  File "{frame.get("filename")}", line {frame.get("lineno")}, in {frame.get("name")}'
            )
        lines.append(f"{exc.get('exc_type')}: {exc.get('exc_value')}")
    return lines


def _extract_log_lines(log_response: dict[str, Any]) -> list[str]:
    lines = []
    for entry in log_response.get("content") or []:
        if not isinstance(entry, dict):
            lines.append(str(entry))
            continue
        level = entry.get("level")
        event = entry.get("event", "")
        lines.append(f"[{level}] {event}" if level else str(event))
        if entry.get("error_detail"):
            lines.extend(_format_error_detail(entry["error_detail"]))
    return lines


async def _fetch_task_log(
    dag_id: str, dag_run_id: str, task_id: str, try_number: int, map_index: int, tail_lines: int | None
) -> dict[str, Any]:
    path = f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    # full_content=True bypasses the API's own pagination so we see the whole log before
    # truncating client-side; the API has no "last N lines" query of its own.
    result = await call_api(
        "GET",
        path,
        params={"map_index": map_index, "full_content": True},
        headers={"Accept": "application/json"},
    )
    lines = _extract_log_lines(result)
    total_lines = len(lines)
    truncated = tail_lines is not None and total_lines > tail_lines
    if tail_lines is not None and truncated:
        # tail_lines is validated >= 1 at the tool boundary and passed as a positive literal
        # internally, so this is always a genuine "last N lines" slice (never lines[-0:]).
        lines = lines[-tail_lines:]
    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "total_lines": total_lines,
        "truncated": truncated,
        "log": "\n".join(lines),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_task_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    map_index: Annotated[int, Field(description="Mapped task index; -1 for an unmapped task.")] = -1,
    tail_lines: Annotated[
        int | None,
        Field(ge=1, description="Keep only the last N lines to protect context; None for the full log."),
    ] = 500,
) -> Any:
    """Get the log of one task instance try, tailed to the last N lines by default.

    The response reports total_lines and truncated so the agent knows if it is seeing a
    partial log; pass tail_lines=None to fetch the log in full.
    """
    return await _fetch_task_log(dag_id, dag_run_id, task_id, try_number, map_index, tail_lines)


@mcp.tool(annotations={"readOnlyHint": True})
async def diagnose_dag_run(dag_id: str, dag_run_id: str) -> Any:
    """One-call debugging summary of a Dag run.

    Returns the run's detail, all its task instances, and -- for each failed or
    upstream_failed task instance -- the last 50 log lines of its latest try. Use this first
    when investigating why a Dag run failed, before pulling individual logs.
    """
    ti_limit = 500
    dag_run = await call_api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}")
    ti_result = await call_api(
        "GET", f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params={"limit": ti_limit}
    )
    task_instances = ti_result.get("task_instances", [])
    total_task_instances = ti_result.get("total_entries")

    failures = []
    for ti in task_instances:
        if ti.get("state") not in _FAILED_TASK_STATES:
            continue
        log = await _fetch_task_log(
            dag_id,
            dag_run_id,
            ti["task_id"],
            try_number=max(ti.get("try_number") or 1, 1),
            map_index=ti.get("map_index", -1),
            tail_lines=50,
        )
        failures.append({"task_id": ti["task_id"], "state": ti["state"], "log_tail": log["log"]})

    return {
        "dag_run": dag_run,
        "task_instances": [_pick(ti, _TASK_INSTANCE_SUMMARY_FIELDS) for ti in task_instances],
        "total_task_instances": total_task_instances,
        "task_instances_truncated": total_task_instances is not None and total_task_instances > ti_limit,
        "failures": failures,
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def list_import_errors(limit: int = 50) -> Any:
    """List Dag file import errors -- the primary tool for debugging why a Dag isn't showing up."""
    return await call_api("GET", "/api/v2/importErrors", params={"limit": limit})


@mcp.tool(annotations={"readOnlyHint": True})
async def list_warnings(limit: int = 50) -> Any:
    """List Dag warnings raised by the scheduler (e.g. non-fatal Dag definition issues)."""
    return await call_api("GET", "/api/v2/dagWarnings", params={"limit": limit})


@mcp.tool(annotations={"readOnlyHint": True})
async def list_variables(limit: int = 50) -> Any:
    """List Airflow Variable keys (never values -- use get_variable for a specific value)."""
    result = await call_api("GET", "/api/v2/variables", params={"limit": limit})
    return {
        "variables": [_pick(v, _VARIABLE_SUMMARY_FIELDS) for v in result.get("variables", [])],
        "total_entries": result.get("total_entries"),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def list_connections(limit: int = 50) -> Any:
    """List Airflow connections, omitting password and extra (which may hold secrets)."""
    result = await call_api("GET", "/api/v2/connections", params={"limit": limit})
    return {
        "connections": [_pick(c, _CONNECTION_SUMMARY_FIELDS) for c in result.get("connections", [])],
        "total_entries": result.get("total_entries"),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def list_pools() -> Any:
    """List worker pools and their slot usage (occupied/running/queued/open slots)."""
    return await call_api("GET", "/api/v2/pools")


@mcp.tool(annotations={"readOnlyHint": True})
async def get_variable(key: str) -> Any:
    """Get a single Airflow Variable, including its value, by key."""
    return await call_api("GET", f"/api/v2/variables/{key}")


@mcp.tool(annotations={"readOnlyHint": True})
async def airflow_api_get(
    path: Annotated[str, Field(description="Absolute API path, e.g. '/api/v2/assets'.")],
    params: Annotated[dict[str, Any] | None, Field(description="Query parameters, if any.")] = None,
) -> Any:
    """Escape hatch for any read-only Airflow REST API endpoint not covered by a dedicated tool.

    `path` must start with '/api/v2/'. See the Airflow REST API reference for available
    endpoints and their query parameters.
    """
    return await call_api("GET", path, params=params)


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False})
async def trigger_dag_run(
    dag_id: str,
    logical_date: Annotated[
        str | None, Field(description="ISO 8601 logical date; omit to let Airflow generate one.")
    ] = None,
    conf: Annotated[dict[str, Any] | None, Field(description="Dag run configuration dict.")] = None,
    note: str | None = None,
) -> Any:
    """Trigger a new Dag run. Requires AIRFLOW_MCP_ALLOW_WRITES=true."""
    body = {"logical_date": logical_date, "conf": conf or {}, "note": note}
    return await call_api("POST", f"/api/v2/dags/{dag_id}/dagRuns", json_body=body)


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True})
async def set_dag_paused(dag_id: str, paused: bool) -> Any:
    """Pause or unpause a Dag. Requires AIRFLOW_MCP_ALLOW_WRITES=true."""
    return await call_api("PATCH", f"/api/v2/dags/{dag_id}", json_body={"is_paused": paused})


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False})
async def clear_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_ids: Annotated[
        list[str] | None, Field(description="Task ids to clear; omit to clear the whole Dag run.")
    ] = None,
    only_failed: bool = True,
    dry_run: Annotated[bool, Field(description="If true, report what would clear without doing it.")] = False,
) -> Any:
    """Clear task instances so they rerun. Requires AIRFLOW_MCP_ALLOW_WRITES=true.

    Defaults to clearing only failed task instances; set only_failed=False to clear
    regardless of state. Pass dry_run=True first to preview the affected task instances.
    """
    body = {
        "dag_run_id": dag_run_id,
        "task_ids": task_ids,
        "only_failed": only_failed,
        "dry_run": dry_run,
    }
    return await call_api("POST", f"/api/v2/dags/{dag_id}/clearTaskInstances", json_body=body)


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
async def airflow_api_call(
    method: Annotated[str, Field(description="HTTP method: POST, PATCH, PUT, or DELETE.")],
    path: Annotated[str, Field(description="Absolute API path, e.g. '/api/v2/dags/my_dag'.")],
    params: dict[str, Any] | None = None,
    json_body: Annotated[dict[str, Any] | None, Field(description="JSON request body, if any.")] = None,
) -> Any:
    """Escape hatch for any write or delete Airflow REST API call not covered by a dedicated tool.

    `path` must start with '/api/v2/'. POST/PATCH/PUT require AIRFLOW_MCP_ALLOW_WRITES=true;
    DELETE requires the stricter, separate AIRFLOW_MCP_ALLOW_DELETES=true (off by default even
    when writes are enabled, because deletions are irreversible).
    """
    return await call_api(method, path, params=params, json_body=json_body)
