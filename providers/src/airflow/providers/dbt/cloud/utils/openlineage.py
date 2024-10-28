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

from airflow import __version__ as airflow_version

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
    from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
    from airflow.providers.openlineage.extractors.base import OperatorLineage


def _get_try_number(val):
    # todo: remove when min airflow version >= 2.10.0
    from packaging.version import parse

    if parse(parse(airflow_version).base_version) < parse("2.10.0"):
        return val.try_number - 1
    else:
        return val.try_number


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
    from openlineage.common.provider.dbt import DbtCloudArtifactProcessor, ParentRunMetadata

    try:
        from airflow.providers.openlineage.conf import namespace
    except ModuleNotFoundError as e:
        from airflow.exceptions import AirflowOptionalProviderFeatureException

        msg = "Please install `apache-airflow-providers-openlineage>=1.7.0`"
        raise AirflowOptionalProviderFeatureException(e, msg)

    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.plugins.adapter import (
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
            job_namespace=namespace(),
            skip_errors=False,
            logger=operator.log,
            manifest=manifest,
            run_result=artifacts["run_results.json"],
            profile=connection,
            catalog=catalog,
        )

        # generate same run id of current task instance
        parent_run_id = OpenLineageAdapter.build_task_instance_run_id(
            dag_id=task_instance.dag_id,
            task_id=operator.task_id,
            execution_date=task_instance.execution_date,
            try_number=_get_try_number(task_instance),
        )

        parent_job = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=f"{task_instance.dag_id}.{task_instance.task_id}",
            job_namespace=namespace(),
        )
        processor.dbt_run_metadata = parent_job

        events = processor.parse().events()

        client = get_openlineage_listener().adapter.get_or_create_openlineage_client()

        for event in events:
            client.emit(event=event)
    return OperatorLineage()
