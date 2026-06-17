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
"""Public helpers for emitting OpenLineage events carrying dataset lineage."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState

from airflow.providers.openlineage.api.core import emit, is_openlineage_active
from airflow.providers.openlineage.plugins.adapter import _PRODUCER, OpenLineageAdapter
from airflow.providers.openlineage.plugins.macros import (
    _get_dag_run_clear_number,
    _get_dag_run_conf,
    _get_logical_date,
    lineage_job_name,
    lineage_job_namespace,
    lineage_run_id,
)
from airflow.providers.openlineage.utils.utils import (
    build_task_event_job_facets,
    build_task_event_run_facets,
    get_dag_run_dag_and_task_from_ti,
    get_task_instance_from_context,
)
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from openlineage.client.event_v2 import InputDataset, OutputDataset
    from openlineage.client.facet_v2 import JobFacet, RunFacet

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

log = logging.getLogger(__name__)

__all__ = ["emit_dataset_lineage"]


def emit_dataset_lineage(
    *,
    inputs: list[InputDataset] | None = None,
    outputs: list[OutputDataset] | None = None,
    task_instance: RuntimeTaskInstance | TaskInstance | None = None,
    additional_run_facets: dict[str, RunFacet] | None = None,
    additional_job_facets: dict[str, JobFacet] | None = None,
    raise_on_error: bool = False,
) -> None:
    """
    Emit an OpenLineage RUNNING event attributing datasets to the current task run.

    This helper lets DAG authors supplement automatic extractor-based lineage with their own datasets.
    It constructs and emits a RUNNING ``RunEvent`` whose run/job identifiers match the currently
    executing Airflow task, attaches the same facets the provider adds to a task's START/COMPLETE events.

    At least one of ``inputs`` or ``outputs`` must be non-empty.

    Helpful References:

    - Dataset naming convention: https://openlineage.io/docs/spec/naming
    - Dataset naming helpers: https://openlineage.io/docs/client/python/best-practices#dataset-naming-helpers
    - Available facets: https://openlineage.io/docs/spec/facets

    :param inputs: Input datasets consumed by the task.
    :param outputs: Output datasets produced by the task.
    :param task_instance: The Airflow task instance to attribute lineage to. Defaults to the currently
        executing task instance obtained from the execution context.
    :param additional_run_facets: Extra run facets to attach.
    :param additional_job_facets: Extra job facets to attach.
    :param raise_on_error: When ``False`` (default), any exception raised while building or emitting
        the event is logged at WARNING level and the function returns silently — so a broken lineage
        helper never breaks a user's task. Set to ``True`` to opt into normal exception propagation.

    :raises ValueError: When ``raise_on_error=True``, if both ``inputs`` and ``outputs`` are empty or
        ``None``.
    :raises TypeError: When ``raise_on_error=True``, if any item in ``inputs`` or ``outputs`` is not
        an OpenLineage ``Dataset``.
    :raises RuntimeError: When ``raise_on_error=True``, if ``task_instance`` is not provided and
        cannot be resolved from the current execution context.

    Example:

    .. code-block:: python

        from openlineage.client.event_v2 import Dataset
        from airflow.providers.openlineage.api import emit_dataset_lineage


        @task
        def my_task():
            emit_dataset_lineage(
                inputs=[Dataset(namespace="s3://bucket", name="raw/2024/01/01/data.csv")],
                outputs=[Dataset(namespace="snowflake://account", name="analytics.public.users")],
            )
    """
    if not is_openlineage_active():
        log.info("OpenLineage is not active - emit_dataset_lineage will have no effect.")
        return

    try:
        if not inputs and not outputs:
            raise ValueError("At least one of `inputs` or `outputs` must be provided.")

        inputs = inputs or []
        outputs = outputs or []
        if not all(isinstance(d, Dataset) for d in (*inputs, *outputs)):
            raise TypeError("`inputs` and `outputs` must contain only OpenLineage Dataset objects.")

        if task_instance is None:
            log.debug("TaskInstance not provided, retrieving it from context.")
            task_instance = get_task_instance_from_context()

        dag_run, dag, task = get_dag_run_dag_and_task_from_ti(task_instance)
        task_uuid = lineage_run_id(task_instance)

        run_facets = build_task_event_run_facets(
            task_instance=task_instance,
            dag_run=dag_run,
            dag=dag,
            task=task,
            task_uuid=task_uuid,
            ti_state=TaskInstanceState.RUNNING,
            parent_run_id=OpenLineageAdapter.build_dag_run_id(
                dag_id=dag.dag_id,
                logical_date=_get_logical_date(task_instance),
                clear_number=_get_dag_run_clear_number(task_instance),
            ),
            parent_job_name=dag.dag_id,
            dr_conf=_get_dag_run_conf(task_instance),
            additional_run_facets=additional_run_facets,
        )
        job_facets = build_task_event_job_facets(
            task=task, dag=dag, additional_job_facets=additional_job_facets
        )

        event = RunEvent(
            eventType=RunState.RUNNING,
            eventTime=datetime.now(tz=timezone.utc).isoformat(),
            run=Run(runId=task_uuid, facets=run_facets),
            job=Job(
                namespace=lineage_job_namespace(),
                name=lineage_job_name(task_instance),
                facets=job_facets,
            ),
            inputs=inputs,
            outputs=outputs,
            producer=_PRODUCER,
        )

        log.info("emit_dataset_lineage will emit a RUNNING OpenLineage event.")
        emit(event)
    except Exception as err:
        if raise_on_error:
            raise
        log.warning("emit_dataset_lineage raised an error `%s`", err)
        log.debug("Exception details:", exc_info=True)
