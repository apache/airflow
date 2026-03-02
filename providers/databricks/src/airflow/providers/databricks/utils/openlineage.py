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

import datetime
import json
import logging
from typing import TYPE_CHECKING, Any

import requests

from airflow.providers.common.compat.openlineage.check import require_openlineage_version
from airflow.utils import timezone

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent
    from openlineage.client.facet_v2 import JobFacet

    from airflow.providers.databricks.hooks.databricks import DatabricksHook
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

log = logging.getLogger(__name__)


def _get_parent_run_facet(task_instance):
    """
    Retrieve the ParentRunFacet associated with a specific Airflow task instance.

    This facet helps link OpenLineage events of child jobs - such as queries executed within
    external systems (e.g., Databricks) by the Airflow task - to the original Airflow task execution.
    Establishing this connection enables better lineage tracking and observability.
    """
    from openlineage.client.facet_v2 import parent_run

    from airflow.providers.openlineage.plugins.macros import (
        lineage_job_name,
        lineage_job_namespace,
        lineage_root_job_name,
        lineage_root_run_id,
        lineage_run_id,
    )

    parent_run_id = lineage_run_id(task_instance)
    parent_job_name = lineage_job_name(task_instance)
    parent_job_namespace = lineage_job_namespace()

    root_parent_run_id = lineage_root_run_id(task_instance)
    rot_parent_job_name = lineage_root_job_name(task_instance)

    try:  # Added in OL provider 2.9.0, try to use it if possible
        from airflow.providers.openlineage.plugins.macros import lineage_root_job_namespace

        root_parent_job_namespace = lineage_root_job_namespace(task_instance)
    except ImportError:
        root_parent_job_namespace = lineage_job_namespace()

    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(
            namespace=parent_job_namespace,
            name=parent_job_name,
        ),
        root=parent_run.Root(
            run=parent_run.RootRun(runId=root_parent_run_id),
            job=parent_run.RootJob(
                name=rot_parent_job_name,
                namespace=root_parent_job_namespace,
            ),
        ),
    )


def _run_api_call(hook: DatabricksSqlHook | DatabricksHook, query_ids: list[str]) -> list[dict]:
    """Retrieve execution details for specific queries from Databricks's query history API."""
    token = hook._get_token(raise_error=True)
    # https://docs.databricks.com/api/azure/workspace/queryhistory/list
    response = requests.get(
        url=f"https://{hook.host}/api/2.0/sql/history/queries",
        headers={"Authorization": f"Bearer {token}"},
        data=json.dumps({"filter_by": {"statement_ids": query_ids}}),
        timeout=3,
    )
    response.raise_for_status()

    return response.json()["res"]


def _process_data_from_api(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert timestamp fields to UTC datetime objects."""
    for row in data:
        for key in ("query_start_time_ms", "query_end_time_ms"):
            row[key] = datetime.datetime.fromtimestamp(row[key] / 1000, tz=datetime.timezone.utc)

    return data


def _get_queries_details_from_databricks(
    hook: DatabricksSqlHook | DatabricksHook, query_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not query_ids:
        return {}

    query_details = {}
    try:
        queries_info_from_api = _run_api_call(hook=hook, query_ids=query_ids)
        queries_info_from_api = _process_data_from_api(queries_info_from_api)

        query_details = {
            query_info["query_id"]: {
                "status": query_info.get("status"),
                "start_time": query_info.get("query_start_time_ms"),
                "end_time": query_info.get("query_end_time_ms"),
                "query_text": query_info.get("query_text"),
                "error_message": query_info.get("error_message"),
            }
            for query_info in queries_info_from_api
            if query_info["query_id"]
        }
    except Exception as e:
        log.info(
            "OpenLineage encountered an error while retrieving additional metadata about SQL queries"
            " from Databricks. The process will continue with default values. Error details: %s",
            e,
        )

    return query_details


def _create_ol_event_pair(
    job_namespace: str,
    job_name: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    is_successful: bool,
    run_facets: dict | None = None,
    job_facets: dict | None = None,
) -> tuple[RunEvent, RunEvent]:
    """Create a pair of OpenLineage RunEvents representing the start and end of a query execution."""
    from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
    from openlineage.client.uuid import generate_new_uuid

    run = Run(runId=str(generate_new_uuid()), facets=run_facets or {})
    job = Job(namespace=job_namespace, name=job_name, facets=job_facets or {})

    start = RunEvent(
        eventType=RunState.START,
        eventTime=start_time.isoformat(),
        run=run,
        job=job,
    )
    end = RunEvent(
        eventType=RunState.COMPLETE if is_successful else RunState.FAIL,
        eventTime=end_time.isoformat(),
        run=run,
        job=job,
    )
    return start, end


@require_openlineage_version(provider_min_version="2.5.0")
def emit_openlineage_events_for_databricks_queries(
    task_instance,
    hook: DatabricksSqlHook | DatabricksHook | None = None,
    query_ids: list[str] | None = None,
    query_source_namespace: str | None = None,
    query_for_extra_metadata: bool = False,
    additional_run_facets: dict | None = None,
    additional_job_facets: dict | None = None,
) -> None:
    """
    Emit OpenLineage events for executed Databricks queries.

    Metadata retrieval from Databricks is attempted only if `get_extra_metadata` is True and hook is provided.
    If metadata is available, execution details such as start time, end time, execution status,
    error messages, and SQL text are included in the events. If no metadata is found, the function
    defaults to using the Airflow task instance's state and the current timestamp.

    Note that both START and COMPLETE event for each query will be emitted at the same time.
    If we are able to query Databricks for query execution metadata, event times
    will correspond to actual query execution times.

    Args:
        task_instance: The Airflow task instance that run these queries.
        hook: A supported Databricks hook instance used to retrieve query metadata if available.
        If omitted, `query_ids` and `query_source_namespace` must be provided explicitly and
        `query_for_extra_metadata` must be `False`.
        query_ids: A list of Databricks query IDs to emit events for, can only be None if `hook` is provided
        and `hook.query_ids` are present (DatabricksHook does not store query_ids).
        query_source_namespace: The namespace to be included in ExternalQueryRunFacet,
        can be `None` only if hook is provided.
        query_for_extra_metadata: Whether to query Databricks for additional metadata about queries.
        Must be `False` if `hook` is not provided.
        additional_run_facets: Additional run facets to include in OpenLineage events.
        additional_job_facets: Additional job facets to include in OpenLineage events.
    """
    from openlineage.client.facet_v2 import job_type_job

    from airflow.providers.common.compat.openlineage.facet import (
        ErrorMessageRunFacet,
        ExternalQueryRunFacet,
        RunFacet,
        SQLJobFacet,
    )
    from airflow.providers.openlineage.conf import namespace
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    log.info("OpenLineage will emit events for Databricks queries.")

    if hook:
        if not query_ids:
            log.debug("No Databricks query IDs provided; Checking `hook.query_ids` property.")
            query_ids = getattr(hook, "query_ids", [])
            if not query_ids:
                raise ValueError("No Databricks query IDs provided and `hook.query_ids` are not present.")

        if not query_source_namespace:
            log.debug("No Databricks query namespace provided; Creating one from scratch.")

            if hasattr(hook, "get_openlineage_database_info") and hasattr(hook, "get_conn_id"):
                from airflow.providers.openlineage.sqlparser import SQLParser

                query_source_namespace = SQLParser.create_namespace(
                    hook.get_openlineage_database_info(hook.get_connection(hook.get_conn_id()))
                )
            else:
                query_source_namespace = f"databricks://{hook.host}" if hook.host else "databricks"
    else:
        if not query_ids:
            raise ValueError("If 'hook' is not provided, 'query_ids' must be set.")
        if not query_source_namespace:
            raise ValueError("If 'hook' is not provided, 'query_source_namespace' must be set.")
        if query_for_extra_metadata:
            raise ValueError("If 'hook' is not provided, 'query_for_extra_metadata' must be False.")

    query_ids = [q for q in query_ids]  # Make a copy to make sure we do not change hook's attribute

    if query_for_extra_metadata and hook:
        log.debug("Retrieving metadata for %s queries from Databricks.", len(query_ids))
        databricks_metadata = _get_queries_details_from_databricks(hook, query_ids)
    else:
        log.debug("`query_for_extra_metadata` is False. No extra metadata fill be fetched from Databricks.")
        databricks_metadata = {}

    # If real metadata is unavailable, we send events with eventTime=now
    default_event_time = timezone.utcnow()
    # ti.state has no `value` attr (AF2) when task it's still running, in AF3 we get 'running', in that case
    # assuming it's user call and query succeeded, so we replace it with success.
    # Adjust state for DBX logic, where "finished" means "success"
    default_state = (
        getattr(task_instance.state, "value", "running") if hasattr(task_instance, "state") else ""
    )
    default_state = "finished" if default_state in ("running", "success") else default_state

    log.debug("Generating OpenLineage facets")
    common_run_facets = {"parent": _get_parent_run_facet(task_instance)}
    common_job_facets: dict[str, JobFacet] = {
        "jobType": job_type_job.JobTypeJobFacet(
            jobType="QUERY",
            integration="DATABRICKS",
            processingType="BATCH",
        )
    }
    additional_run_facets = additional_run_facets or {}
    additional_job_facets = additional_job_facets or {}

    events: list[RunEvent] = []
    for counter, query_id in enumerate(query_ids, 1):
        query_metadata = databricks_metadata.get(query_id, {})
        log.debug(
            "Metadata for query no. %s, (ID `%s`): `%s`",
            counter,
            query_id,
            query_metadata if query_metadata else "not found",
        )

        query_specific_run_facets: dict[str, RunFacet] = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=query_id, source=query_source_namespace)
        }
        if query_metadata.get("error_message"):
            query_specific_run_facets["error"] = ErrorMessageRunFacet(
                message=query_metadata["error_message"],
                programmingLanguage="SQL",
            )

        query_specific_job_facets = {}
        if query_metadata.get("query_text"):
            query_specific_job_facets["sql"] = SQLJobFacet(query=query_metadata["query_text"])

        log.debug("Creating OpenLineage event pair for query ID: %s", query_id)
        event_batch = _create_ol_event_pair(
            job_namespace=namespace(),
            job_name=f"{task_instance.dag_id}.{task_instance.task_id}.query.{counter}",
            start_time=query_metadata.get("start_time") or default_event_time,
            end_time=query_metadata.get("end_time") or default_event_time,
            # Only finished status means it completed without failures
            is_successful=(query_metadata.get("status") or default_state).lower() == "finished",
            run_facets={**query_specific_run_facets, **common_run_facets, **additional_run_facets},
            job_facets={**query_specific_job_facets, **common_job_facets, **additional_job_facets},
        )
        events.extend(event_batch)

    log.debug("Generated %s OpenLineage events; emitting now.", len(events))
    adapter = get_openlineage_listener().adapter
    for event in events:
        adapter.emit(event)

    log.info("OpenLineage has successfully finished processing information about Databricks queries.")
    return
