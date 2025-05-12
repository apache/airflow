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
from airflow.providers.databricks.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent
    from openlineage.client.facet_v2 import JobFacet

    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

log = logging.getLogger(__name__)


def _get_logical_date(task_instance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        dagrun = task_instance.get_template_context()["dag_run"]
        return dagrun.logical_date or dagrun.run_after

    if hasattr(task_instance, "logical_date"):
        date = task_instance.logical_date
    else:
        date = task_instance.execution_date

    return date


def _get_dag_run_clear_number(task_instance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        dagrun = task_instance.get_template_context()["dag_run"]
        return dagrun.clear_number
    return task_instance.dag_run.clear_number


# todo: move this run_id logic into OpenLineage's listener to avoid differences
def _get_ol_run_id(task_instance) -> str:
    """
    Get OpenLineage run_id from TaskInstance.

    It's crucial that the task_instance's run_id creation logic matches OpenLineage's listener implementation.
    Only then can we ensure that the generated run_id aligns with the Airflow task,
    enabling a proper connection between events.
    """
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    # Generate same OL run id as is generated for current task instance
    return OpenLineageAdapter.build_task_instance_run_id(
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        logical_date=_get_logical_date(task_instance),
        try_number=task_instance.try_number,
        map_index=task_instance.map_index,
    )


# todo: move this run_id logic into OpenLineage's listener to avoid differences
def _get_ol_dag_run_id(task_instance) -> str:
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    return OpenLineageAdapter.build_dag_run_id(
        dag_id=task_instance.dag_id,
        logical_date=_get_logical_date(task_instance),
        clear_number=_get_dag_run_clear_number(task_instance),
    )


def _get_parent_run_facet(task_instance):
    """
    Retrieve the ParentRunFacet associated with a specific Airflow task instance.

    This facet helps link OpenLineage events of child jobs - such as queries executed within
    external systems (e.g., Databricks) by the Airflow task - to the original Airflow task execution.
    Establishing this connection enables better lineage tracking and observability.
    """
    from openlineage.client.facet_v2 import parent_run

    from airflow.providers.openlineage.conf import namespace

    parent_run_id = _get_ol_run_id(task_instance)
    root_parent_run_id = _get_ol_dag_run_id(task_instance)

    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(
            namespace=namespace(),
            name=f"{task_instance.dag_id}.{task_instance.task_id}",
        ),
        root=parent_run.Root(
            run=parent_run.RootRun(runId=root_parent_run_id),
            job=parent_run.RootJob(
                name=task_instance.dag_id,
                namespace=namespace(),
            ),
        ),
    )


def _run_api_call(hook: DatabricksSqlHook, query_ids: list[str]) -> list[dict]:
    """Retrieve execution details for specific queries from Databricks's query history API."""
    if not hook._token:
        # This has logic for token initialization
        hook.get_conn()

    # https://docs.databricks.com/api/azure/workspace/queryhistory/list
    try:
        response = requests.get(
            url=f"https://{hook.host}/api/2.0/sql/history/queries",
            headers={"Authorization": f"Bearer {hook._token}"},
            data=json.dumps({"filter_by": {"statement_ids": query_ids}}),
            timeout=2,
        )
    except Exception as e:
        log.warning(
            "OpenLineage could not retrieve Databricks queries details. Error received: `%s`.",
            e,
        )
        return []

    if response.status_code != 200:
        log.warning(
            "OpenLineage could not retrieve Databricks queries details. API error received: `%s`: `%s`",
            response.status_code,
            response.text,
        )
        return []

    return response.json()["res"]


def _get_queries_details_from_databricks(
    hook: DatabricksSqlHook, query_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not query_ids:
        return {}

    queries_info_from_api = _run_api_call(hook=hook, query_ids=query_ids)

    query_details = {}
    for query_info in queries_info_from_api:
        if not query_info.get("query_id"):
            log.debug("Databricks query ID not found in API response.")
            continue

        q_start_time = None
        q_end_time = None
        if query_info.get("query_start_time_ms") and query_info.get("query_end_time_ms"):
            q_start_time = datetime.datetime.fromtimestamp(
                query_info["query_start_time_ms"] / 1000, tz=datetime.timezone.utc
            )
            q_end_time = datetime.datetime.fromtimestamp(
                query_info["query_end_time_ms"] / 1000, tz=datetime.timezone.utc
            )

        query_details[query_info["query_id"]] = {
            "status": query_info.get("status"),
            "start_time": q_start_time,
            "end_time": q_end_time,
            "query_text": query_info.get("query_text"),
            "error_message": query_info.get("error_message"),
        }

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


@require_openlineage_version(provider_min_version="2.3.0")
def emit_openlineage_events_for_databricks_queries(
    query_ids: list[str],
    query_source_namespace: str,
    task_instance,
    hook: DatabricksSqlHook | None = None,
    additional_run_facets: dict | None = None,
    additional_job_facets: dict | None = None,
) -> None:
    """
    Emit OpenLineage events for executed Databricks queries.

    Metadata retrieval from Databricks is attempted only if a `DatabricksSqlHook` is provided.
    If metadata is available, execution details such as start time, end time, execution status,
    error messages, and SQL text are included in the events. If no metadata is found, the function
    defaults to using the Airflow task instance's state and the current timestamp.

    Note that both START and COMPLETE event for each query will be emitted at the same time.
    If we are able to query Databricks for query execution metadata, event times
    will correspond to actual query execution times.

    Args:
        query_ids: A list of Databricks query IDs to emit events for.
        query_source_namespace: The namespace to be included in ExternalQueryRunFacet.
        task_instance: The Airflow task instance that run these queries.
        hook: A hook instance used to retrieve query metadata if available.
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

    if not query_ids:
        log.debug("No Databricks query IDs provided; skipping OpenLineage event emission.")
        return

    query_ids = [q for q in query_ids]  # Make a copy to make sure it does not change

    if hook:
        log.debug("Retrieving metadata for %s queries from Databricks.", len(query_ids))
        databricks_metadata = _get_queries_details_from_databricks(hook, query_ids)
    else:
        log.debug("DatabricksSqlHook not provided. No extra metadata fill be fetched from Databricks.")
        databricks_metadata = {}

    # If real metadata is unavailable, we send events with eventTime=now
    default_event_time = timezone.utcnow()
    # If no query metadata is provided, we use task_instance's state when checking for success
    # Adjust state for DBX logic, where "finished" means "success"
    default_state = task_instance.state.value if hasattr(task_instance, "state") else ""
    default_state = "finished" if default_state == "success" else default_state

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
            start_time=query_metadata.get("start_time", default_event_time),  # type: ignore[arg-type]
            end_time=query_metadata.get("end_time", default_event_time),  # type: ignore[arg-type]
            # Only finished status means it completed without failures
            is_successful=query_metadata.get("status", default_state).lower() == "finished",
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
