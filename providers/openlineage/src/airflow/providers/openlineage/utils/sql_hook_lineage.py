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
"""Utilities for processing hook-level lineage into OpenLineage events."""

from __future__ import annotations

import datetime as dt
import logging

from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import external_query_run, job_type_job, sql_job
from openlineage.client.uuid import generate_new_uuid

from airflow.providers.common.compat.sdk import timezone
from airflow.providers.common.sql.hooks.lineage import SqlJobHookLineageExtra
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
from airflow.providers.openlineage.plugins.macros import (
    _get_logical_date,
    lineage_job_name,
    lineage_job_namespace,
    lineage_root_job_name,
    lineage_root_job_namespace,
    lineage_root_run_id,
    lineage_run_id,
)
from airflow.providers.openlineage.sqlparser import SQLParser, get_openlineage_facets_with_sql
from airflow.providers.openlineage.utils.utils import _get_parent_run_facet

log = logging.getLogger(__name__)


def emit_lineage_from_sql_extras(task_instance, sql_extras: list, is_successful: bool = True) -> None:
    """
    Process ``sql_job`` extras and emit per-query OpenLineage events.

    For each extra that contains sql text or job id:

    * Parse SQL via :func:`get_openlineage_facets_with_sql` to obtain inputs,
      outputs and facets (schema enrichment, column lineage, etc.).
    * Emit a separate START + COMPLETE/FAIL event pair (child job of the task).
    """
    if not sql_extras:
        return None

    log.info("OpenLineage will process %s SQL hook lineage extra(s).", len(sql_extras))

    common_job_facets: dict = {
        "jobType": job_type_job.JobTypeJobFacet(
            jobType="QUERY",
            integration="AIRFLOW",
            processingType="BATCH",
        )
    }

    events: list[RunEvent] = []
    query_count = 0

    for extra_info in sql_extras:
        value = extra_info.value

        sql_text = value.get(SqlJobHookLineageExtra.VALUE__SQL_STATEMENT.value, "")
        job_id = value.get(SqlJobHookLineageExtra.VALUE__JOB_ID.value)

        if not sql_text and not job_id:
            log.debug("SQL extra has no SQL text and no job ID, skipping.")
            continue
        query_count += 1

        hook = extra_info.context
        conn_id = _get_hook_conn_id(hook)
        namespace = _resolve_namespace(hook, conn_id)

        # Parse SQL to obtain lineage (inputs, outputs, facets)
        query_lineage: OperatorLineage | None = None
        if sql_text and conn_id:
            try:
                query_lineage = get_openlineage_facets_with_sql(
                    hook=hook,
                    sql=sql_text,
                    conn_id=conn_id,
                    database=value.get(SqlJobHookLineageExtra.VALUE__DEFAULT_DB.value),
                    use_connection=False,  # Temporary solution before we figure out timeouts for queries
                )
            except Exception as e:
                log.debug("Failed to parse SQL for query %s: %s", query_count, e)

        # If parsing SQL failed, just attach SQL text as a facet
        if query_lineage is None:
            job_facets: dict = {}
            if sql_text:
                job_facets["sql"] = sql_job.SQLJobFacet(query=SQLParser.normalize_sql(sql_text))
            query_lineage = OperatorLineage(job_facets=job_facets)

        # Enrich run facets with external query info when available.
        if job_id and namespace:
            query_lineage.run_facets.setdefault(
                "externalQuery",
                external_query_run.ExternalQueryRunFacet(
                    externalQueryId=str(job_id),
                    source=namespace,
                ),
            )

        events.extend(
            _create_ol_event_pair(
                task_instance=task_instance,
                job_name=f"{task_instance.dag_id}.{task_instance.task_id}.query.{query_count}",
                is_successful=is_successful,
                inputs=query_lineage.inputs,
                outputs=query_lineage.outputs,
                run_facets=query_lineage.run_facets,
                job_facets={**common_job_facets, **query_lineage.job_facets},
            )
        )

    if events:
        log.debug("Emitting %s OpenLineage event(s) for SQL hook lineage.", len(events))
        try:
            adapter = get_openlineage_listener().adapter
            for event in events:
                adapter.emit(event)
        except Exception as e:
            log.warning("Failed to emit OpenLineage events for SQL hook lineage: %s", e)
            log.debug("Emission failure details:", exc_info=True)

    return None


def _resolve_namespace(hook, conn_id: str | None) -> str | None:
    """
    Resolve the OpenLineage namespace from a hook.

    Tries ``hook.get_openlineage_database_info`` to build the namespace.
    Returns ``None`` when the hook does not expose this method.
    """
    if conn_id:
        try:
            connection = hook.get_connection(conn_id)
            database_info = hook.get_openlineage_database_info(connection)
        except Exception as e:
            log.debug("Failed to get OpenLineage database info: %s", e)
            database_info = None

        if database_info is not None:
            return SQLParser.create_namespace(database_info)

    return None


def _get_hook_conn_id(hook) -> str | None:
    """
    Try to extract the connection ID from a hook instance.

    Checks for ``get_conn_id()`` first, then falls back to the attribute
    named by ``hook.conn_name_attr``.
    """
    if callable(getattr(hook, "get_conn_id", None)):
        return hook.get_conn_id()
    conn_name_attr = getattr(hook, "conn_name_attr", None)
    if conn_name_attr:
        return getattr(hook, conn_name_attr, None)
    return None


def _create_ol_event_pair(
    task_instance,
    job_name: str,
    is_successful: bool,
    inputs: list | None = None,
    outputs: list | None = None,
    run_facets: dict | None = None,
    job_facets: dict | None = None,
    event_time: dt.datetime | None = None,
) -> tuple[RunEvent, RunEvent]:
    """
    Create a START + COMPLETE/FAIL child event pair linked to a task instance.

    Handles parent-run facet generation, run-ID creation and event timestamps
    so callers only need to supply the query-specific facets and datasets.
    """
    parent_facets = _get_parent_run_facet(
        parent_run_id=lineage_run_id(task_instance),
        parent_job_name=lineage_job_name(task_instance),
        parent_job_namespace=lineage_job_namespace(),
        root_parent_run_id=lineage_root_run_id(task_instance),
        root_parent_job_name=lineage_root_job_name(task_instance),
        root_parent_job_namespace=lineage_root_job_namespace(task_instance),
    )

    run = Run(
        runId=str(generate_new_uuid(instant=_get_logical_date(task_instance))),
        facets={**parent_facets, **(run_facets or {})},
    )
    job = Job(namespace=lineage_job_namespace(), name=job_name, facets=job_facets or {})
    event_time = event_time or timezone.utcnow()
    start = RunEvent(
        eventType=RunState.START,
        eventTime=event_time.isoformat(),
        run=run,
        job=job,
        inputs=inputs or [],
        outputs=outputs or [],
    )
    end = RunEvent(
        eventType=RunState.COMPLETE if is_successful else RunState.FAIL,
        eventTime=event_time.isoformat(),
        run=run,
        job=job,
        inputs=inputs or [],
        outputs=outputs or [],
    )
    return start, end
