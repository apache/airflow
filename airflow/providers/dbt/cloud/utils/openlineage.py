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
import re
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
    from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
    from airflow.providers.openlineage.extractors.base import OperatorLineage


def generate_openlineage_events_from_dbt_cloud_run(
    operator: DbtCloudRunJobOperator | DbtCloudJobRunSensor, task_instance: TaskInstance
) -> OperatorLineage:
    """
    Common method generating OpenLineage events from the DBT Cloud run.

    This function retrieves information about a DBT Cloud run, including the associated job,
    project, and execution details. It processes the run's artifacts, such as the manifest and run results,
    in parallel for many steps.
    Then it generates and emits OpenLineage events based on the executed DBT tasks.

    :param operator: Instance of DBT Cloud operator that executed DBT tasks.
        It already should have run_id and dbt cloud hook.
    :param task_instance: Currently executed task instance

    :return: An empty OperatorLineage object indicating the completion of events generation.
    """
    from openlineage.common.provider.dbt import DbtCloudArtifactProcessor, ParentRunMetadata

    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.plugins.adapter import (
        _DAG_NAMESPACE,
        _PRODUCER,
        OpenLineageAdapter,
    )
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    # if no account_id set this will fallback
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
    with suppress(Exception):
        catalog = operator.hook.get_job_run_artifact(operator.run_id, path="catalog.json").json()["data"]

    async def get_artifacts_for_steps(steps, artifacts):
        """Gets artifacts for a list of steps concurrently."""
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
    step_artifacts = asyncio.run(
        get_artifacts_for_steps(steps=steps, artifacts=["manifest.json", "run_results.json"])
    )

    # process each step in loop, sending generated events in the same order as steps
    for artifacts in step_artifacts:
        # process manifest
        manifest = artifacts["manifest.json"]

        if not artifacts.get("run_results.json", None):
            continue

        processor = DbtCloudArtifactProcessor(
            producer=_PRODUCER,
            job_namespace=_DAG_NAMESPACE,
            skip_errors=False,
            logger=operator.log,
            manifest=manifest,
            run_result=artifacts["run_results.json"],
            profile=connection,
            catalog=catalog,
        )

        # generate same run id of current task instance
        parent_run_id = OpenLineageAdapter.build_task_instance_run_id(
            operator.task_id, task_instance.execution_date, task_instance.try_number - 1
        )

        parent_job = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=f"{task_instance.dag_id}.{task_instance.task_id}",
            job_namespace=_DAG_NAMESPACE,
        )
        processor.dbt_run_metadata = parent_job

        events = processor.parse().events()

        client = get_openlineage_listener().adapter.get_or_create_openlineage_client()

        for event in events:
            client.emit(event=event)
    return OperatorLineage()
