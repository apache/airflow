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
"""Public helpers for emitting OpenLineage events describing SQL query executions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from openlineage.client.facet_v2 import error_message_run, external_query_run, sql_job

from airflow.providers.openlineage.api.core import emit, is_openlineage_active
from airflow.providers.openlineage.plugins.adapter import _PRODUCER
from airflow.providers.openlineage.plugins.macros import lineage_job_name
from airflow.providers.openlineage.utils.sql_hook_lineage import (
    _create_ol_event_pair,
    _parse_query_into_datasets,
)
from airflow.providers.openlineage.utils.utils import (
    get_task_instance_from_context,
    next_query_counter_from_context,
)

if TYPE_CHECKING:
    from datetime import datetime

    from openlineage.client.event_v2 import Dataset
    from openlineage.client.facet_v2 import JobFacet, RunFacet

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

log = logging.getLogger(__name__)

__all__ = ["emit_query_lineage"]


def emit_query_lineage(
    *,
    query_id: str | None = None,
    query_source_namespace: str | None = None,
    query_text: str | None = None,
    inputs: list[Dataset] | None = None,
    outputs: list[Dataset] | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    is_successful: bool = True,
    error_message: str | None = None,
    default_database: str | None = None,
    default_schema: str | None = None,
    job_name: str | None = None,
    task_instance: TaskInstance | RuntimeTaskInstance | None = None,
    additional_run_facets: dict[str, RunFacet] | None = None,
    additional_job_facets: dict[str, JobFacet] | None = None,
    raise_on_error: bool = False,
) -> None:
    """
    Emit a START + COMPLETE/FAIL OpenLineage event pair describing a single SQL query execution.

    The emitted events carry a ``parent`` run facet pointing at the currently executing Airflow task
    run. Any OpenLineage root information present on the task instance is propagated to the
    emitted events so the entire run hierarchy stays connected. This helper can be called multiple
    times within a single task; each call produces a distinct query event pair identified by a
    sequential job name suffix (``<dag_id>.<task_id>.query.<n>``).

    :param query_id: Unique identifier of the query in the given ``query_source_namespace``. When both
        ``query_id`` and ``query_source_namespace`` are provided, an ``externalQuery`` run facet is
        attached to the emitted events.
    :param query_source_namespace: OpenLineage namespace of the system that ran the query, e.g.
        ``"snowflake://org-acct"``, ``"databricks://adb-<id>.azuredatabricks.net"``, ``"bigquery"``.
    :param query_text: Raw SQL query text. When provided, it is attached via a ``sql`` JobFacet and
        ``inputs``/``outputs`` explicitly supplied are enriched with datasets retrieved from query parsing.
    :param inputs: Additional input datasets.
    :param outputs: Additional output datasets.
    :param start_time: Event time of the START event. Defaults to the current UTC time.
    :param end_time: Event time of the COMPLETE/FAIL event. Defaults to the current UTC time.
    :param is_successful: Whether the query completed successfully (COMPLETE) or failed (FAIL).
    :param error_message: Optional error message attached as an ``errorMessage`` run facet.
    :param default_database: Default database for resolving unqualified tables in ``query_text``.
    :param default_schema: Default schema for resolving unqualified tables in ``query_text``.
    :param job_name: Job name to use in both events. Defaults to <ti_job_name>.manual_query.<counter>.
    :param task_instance: The Airflow task instance to attribute the query to. Defaults to the
        currently executing task instance obtained from the execution context.
    :param additional_run_facets: Extra run facets to merge into the emitted events.
    :param additional_job_facets: Extra job facets to merge into the emitted events.
    :param raise_on_error: When ``False`` (default), any exception raised while building or emitting
        the events is logged at WARNING level and the function returns silently — so a broken lineage
        helper never breaks a user's task. Set to ``True`` to opt into normal exception propagation.

    :raises RuntimeError: When ``raise_on_error=True``, if ``task_instance`` is not provided and
        cannot be resolved from the current execution context.

    Example:

    .. code-block:: python

        from airflow.providers.openlineage.api import emit_query_lineage


        @task
        def my_task():
            emit_query_lineage(
                query_id="acde070d-8c4c-4f0d-9d8a-162843c10333",
                query_source_namespace="databricks://adb-498971240325220.10.azuredatabricks.net",
                query_text="SELECT * FROM analytics.public.users",
            )
    """
    if not is_openlineage_active():
        log.info("OpenLineage is not active - emit_query_lineage will have no effect.")
        return

    try:
        if task_instance is None:
            log.debug("TaskInstance not provided, retrieving it from context.")
            task_instance = get_task_instance_from_context()

        # Copy caller-supplied lists so we never mutate user inputs.
        all_inputs = list(inputs) if inputs else []
        all_outputs = list(outputs) if outputs else []
        # Parse SQL into datasets and enrich inputs/outputs.
        if query_text and query_source_namespace:
            query_inputs, query_outputs = _parse_query_into_datasets(
                query_text=query_text,
                query_source_namespace=query_source_namespace,
                default_database=default_database,
                default_schema=default_schema,
            )
            all_inputs.extend(query_inputs)
            all_outputs.extend(query_outputs)

        # Run facets: user-provided first, internal facets overlaid so internals always win.
        run_facets: dict[str, RunFacet] = {**(additional_run_facets or {})}
        if query_id and query_source_namespace:
            run_facets["externalQuery"] = external_query_run.ExternalQueryRunFacet(
                externalQueryId=query_id, source=query_source_namespace, producer=_PRODUCER
            )
        if error_message:
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="SQL", producer=_PRODUCER
            )

        # Job facets: user-provided first, internal facets overlaid so internals always win.
        job_facets: dict[str, JobFacet] = {**(additional_job_facets or {})}
        if query_text:
            job_facets["sql"] = sql_job.SQLJobFacet(query=query_text, producer=_PRODUCER)

        if job_name is None:
            job_name = f"{lineage_job_name(task_instance)}.manual_query.{next_query_counter_from_context()}"

        start_event, end_event = _create_ol_event_pair(
            task_instance=task_instance,
            job_name=job_name,
            is_successful=is_successful,
            inputs=all_inputs,
            outputs=all_outputs,
            run_facets=run_facets,
            job_facets=job_facets,
            start_event_time=start_time,
            end_event_time=end_time,
        )

        log.info("emit_query_lineage will emit 2 OpenLineage events for job `%s`.", start_event.job.name)
        emit(start_event)
        emit(end_event)
    except Exception as err:
        if raise_on_error:
            raise
        log.warning("emit_query_lineage raised an error `%s`", err)
        log.debug("Exception details:", exc_info=True)
