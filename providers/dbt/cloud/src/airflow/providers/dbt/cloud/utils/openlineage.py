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

import asyncio
import logging
import re
from typing import TYPE_CHECKING

from airflow.providers.common.compat.openlineage.check import require_openlineage_version
from airflow.providers.dbt.cloud.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
    from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
    from airflow.providers.openlineage.extractors.base import OperatorLineage


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


def _get_parent_run_metadata(task_instance):
    """
    Retrieve the ParentRunMetadata associated with a specific Airflow task instance.

    This metadata helps link OpenLineage events of child jobs to the original Airflow task execution.
    Establishing this connection enables better lineage tracking and observability.
    """
    from openlineage.common.provider.dbt import ParentRunMetadata

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

    return ParentRunMetadata(
        run_id=parent_run_id,
        job_name=parent_job_name,
        job_namespace=parent_job_namespace,
        root_parent_run_id=root_parent_run_id,
        root_parent_job_name=rot_parent_job_name,
        root_parent_job_namespace=root_parent_job_namespace,
    )


@require_openlineage_version(provider_min_version="2.5.0")
def generate_openlineage_events_from_dbt_cloud_run(
    operator: DbtCloudRunJobOperator | DbtCloudJobRunSensor, task_instance: TaskInstance
) -> OperatorLineage:
    """
    Generate OpenLineage events from the DBT Cloud run.

    This function retrieves information about a DBT Cloud run, including the associated job,
    project, and execution details. It processes the run's artifacts, such as the manifest and run results,
    in parallel for many steps.
    Then it generates and emits OpenLineage events based on the executed DBT tasks.

    :param operator: Instance of DBT Cloud operator that executed DBT tasks.
        It already should have run_id and dbt cloud hook.
    :param task_instance: Currently executed task instance

    :return: An empty OperatorLineage object indicating the completion of events generation.
    """
    from openlineage.common.provider.dbt import DbtCloudArtifactProcessor

    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.plugins.adapter import _PRODUCER
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    # if no account_id set this will fallback
    log.debug("Retrieving information about DBT job run.")
    job_run = operator.hook.get_job_run(
        run_id=operator.run_id, account_id=operator.account_id, include_related=["run_steps,job"]
    ).json()["data"]
    job = job_run["job"]
    # retrieve account_id from job and use that starting from this line
    account_id = job["account_id"]
    project = operator.hook.get_project(project_id=job["project_id"], account_id=account_id).json()["data"]
    connection = project["connection"]
    execute_steps = job["execute_steps"]
    run_steps = job_run["run_steps"]

    log.debug("Filtering only DBT invocation steps for further processing.")
    # filter only dbt invocation steps
    steps = []
    for run_step in run_steps:
        name = run_step["name"]
        if name.startswith("Invoke dbt with `"):
            regex_pattern = "Invoke dbt with `([^`.]*)`"
            m = re.search(regex_pattern, name)
            if m and m.group(1) in execute_steps:
                steps.append(run_step["index"])

    # catalog is available only if docs are generated
    catalog = None
    try:
        log.debug("Retrieving information about catalog artifact from DBT.")
        catalog = operator.hook.get_job_run_artifact(operator.run_id, path="catalog.json").json()["data"]
    except Exception:
        log.info(
            "Openlineage could not find DBT catalog artifact, usually available when docs are generated."
            "Proceeding with metadata extraction. "
            "If you see error logs above about `HTTP error: Not Found` it's safe to ignore them."
        )

    async def get_artifacts_for_steps(steps, artifacts):
        """Get artifacts for a list of steps concurrently."""
        tasks = [
            operator.hook.get_job_run_artifacts_concurrently(
                run_id=operator.run_id,
                account_id=account_id,
                step=step,
                artifacts=artifacts,
            )
            for step in steps
        ]
        return await asyncio.gather(*tasks)

    # get artifacts for steps concurrently
    log.debug("Retrieving information about artifacts for all job steps from DBT.")
    step_artifacts = asyncio.run(
        get_artifacts_for_steps(steps=steps, artifacts=["manifest.json", "run_results.json"])
    )

    log.debug("Preparing OpenLineage parent job information to be included in DBT events.")
    parent_metadata = _get_parent_run_metadata(task_instance)
    adapter = get_openlineage_listener().adapter

    # process each step in loop, sending generated events in the same order as steps
    for counter, artifacts in enumerate(step_artifacts, 1):
        log.debug("Parsing information about artifact no. %s.", counter)

        # process manifest
        manifest = artifacts["manifest.json"]

        if not artifacts.get("run_results.json", None):
            log.debug("No run results found for artifact no. %s. Skipping.", counter)
            continue

        processor = DbtCloudArtifactProcessor(
            producer=_PRODUCER,
            job_namespace=parent_metadata.job_namespace,
            skip_errors=False,
            logger=operator.log,
            manifest=manifest,
            run_result=artifacts["run_results.json"],
            profile=connection,
            catalog=catalog,
        )

        processor.dbt_run_metadata = parent_metadata

        events = processor.parse().events()
        log.debug("Found %s OpenLineage events for artifact no. %s.", len(events), counter)

        for event in events:
            adapter.emit(event=event)
        log.debug("Emitted all OpenLineage events for artifact no. %s.", counter)

    log.info("OpenLineage has successfully finished processing information about DBT job run.")
    return OperatorLineage()
