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

import os
import traceback
from contextlib import ExitStack
from typing import TYPE_CHECKING, Literal

import yaml
from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    JobFacet,
    RunFacet,
    documentation_job,
    error_message_run,
    job_type_job,
    nominal_time_run,
    ownership_job,
    tags_job,
)
from openlineage.client.uuid import generate_static_uuid

from airflow.configuration import conf as airflow_conf
from airflow.providers.openlineage import __version__ as OPENLINEAGE_PROVIDER_VERSION, conf
from airflow.providers.openlineage.utils.utils import (
    OpenLineageRedactor,
    get_airflow_debug_facet,
    get_airflow_state_run_facet,
    get_processing_engine_facet,
)
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from airflow.observability.stats import Stats
except ImportError:
    from airflow.stats import Stats  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.sdk.execution_time.secrets_masker import SecretsMasker, _secrets_masker
    from airflow.utils.state import DagRunState
else:
    try:
        from airflow.sdk._shared.secrets_masker import SecretsMasker, _secrets_masker
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import SecretsMasker, _secrets_masker
        except ImportError:
            from airflow.utils.log.secrets_masker import SecretsMasker, _secrets_masker

_PRODUCER = f"https://github.com/apache/airflow/tree/providers-openlineage/{OPENLINEAGE_PROVIDER_VERSION}"

set_producer(_PRODUCER)

_JOB_TYPE_DAG: Literal["DAG"] = "DAG"
_JOB_TYPE_TASK: Literal["TASK"] = "TASK"


class OpenLineageAdapter(LoggingMixin):
    """Translate Airflow metadata to OpenLineage events instead of creating them from Airflow code."""

    def __init__(self, client: OpenLineageClient | None = None, secrets_masker: SecretsMasker | None = None):
        super().__init__()
        self._client = client
        if not secrets_masker:
            secrets_masker = _secrets_masker()
        self._redacter = OpenLineageRedactor.from_masker(secrets_masker)

    def get_or_create_openlineage_client(self) -> OpenLineageClient:
        if not self._client:
            # If not already set explicitly - propagate airflow logging level to OpenLineage client
            airflow_core_log_level = airflow_conf.get("logging", "logging_level", fallback="INFO")
            if not os.getenv("OPENLINEAGE_CLIENT_LOGGING") and airflow_core_log_level != "INFO":
                os.environ["OPENLINEAGE_CLIENT_LOGGING"] = airflow_core_log_level

            config = self.get_openlineage_config()
            if config:
                self.log.debug(
                    "OpenLineage configuration found. Transport type: `%s`",
                    config.get("transport", {}).get("type", "no type provided"),
                )
                self._client = OpenLineageClient(config=config)
            else:
                self.log.debug(
                    "OpenLineage configuration not found directly in Airflow. "
                    "Looking for legacy environment configuration. "
                )
                self._client = OpenLineageClient()
        return self._client

    def get_openlineage_config(self) -> dict | None:
        # First, try to read from YAML file
        openlineage_config_path = conf.config_path(check_legacy_env_var=False)
        if openlineage_config_path:
            config = self._read_yaml_config(openlineage_config_path)
            return config
        self.log.debug("OpenLineage config_path configuration not found.")

        # Second, try to get transport config
        transport_config = conf.transport()
        if not transport_config:
            self.log.debug("OpenLineage transport configuration not found.")
            return None
        return {"transport": transport_config}

    @staticmethod
    def _read_yaml_config(path: str) -> dict | None:
        with open(path) as config_file:
            return yaml.safe_load(config_file)

    @staticmethod
    def build_dag_run_id(dag_id: str, logical_date: datetime, clear_number: int) -> str:
        return str(
            generate_static_uuid(
                instant=logical_date,
                data=f"{conf.namespace()}.{dag_id}.{clear_number}".encode(),
            )
        )

    @staticmethod
    def build_task_instance_run_id(
        dag_id: str,
        task_id: str,
        try_number: int,
        logical_date: datetime,
        map_index: int,
    ):
        return str(
            generate_static_uuid(
                instant=logical_date,
                data=f"{conf.namespace()}.{dag_id}.{task_id}.{try_number}.{map_index}".encode(),
            )
        )

    def emit(self, event: RunEvent):
        """
        Emit OpenLineage event.

        :param event: Event to be emitted.
        :return: Redacted Event.
        """
        if not self._client:
            self._client = self.get_or_create_openlineage_client()
        redacted_event: RunEvent = self._redacter.redact(event, max_depth=20)  # type: ignore[assignment]
        event_type = event.eventType.value.lower() if event.eventType else ""
        transport_type = f"{self._client.transport.kind}".lower()

        try:
            with ExitStack() as stack:
                stack.enter_context(Stats.timer(f"ol.emit.attempts.{event_type}.{transport_type}"))
                stack.enter_context(Stats.timer("ol.emit.attempts"))
                self._client.emit(redacted_event)
                self.log.info(
                    "Successfully emitted OpenLineage `%s` event of id `%s`",
                    event_type.upper(),
                    event.run.runId,
                )
        except Exception as e:
            Stats.incr("ol.emit.failed")
            self.log.warning(
                "Failed to emit OpenLineage `%s` event of id `%s` with the following exception: `%s`",
                event_type.upper(),
                event.run.runId,
                e,
            )
            self.log.debug("OpenLineage emission failure details:", exc_info=True)

        return redacted_event

    def start_task(
        self,
        run_id: str,
        job_name: str,
        event_time: str,
        job_description: str | None,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        owners: list[str] | None,
        tags: list[str] | None,
        task: OperatorLineage | None,
        job_description_type: str | None = None,
        run_facets: dict[str, RunFacet] | None = None,
    ) -> RunEvent:
        """
        Emit openlineage event of type START.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task in dag
        :param job_description: description of the job
        :param job_description_type: MIME type of the description arg content
        :param event_time:
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param owners: list of owners
        :param tags: list of tags
        :param task: metadata container with information extracted from operator
        :param run_facets: custom run facets
        """
        run_facets = run_facets or {}
        if task:
            run_facets = {**task.run_facets, **run_facets}
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id=run_id,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name=job_name,
                job_type=_JOB_TYPE_TASK,
                job_description=job_description,
                job_description_type=job_description_type,
                job_owners=owners,
                job_tags=tags,
                job_facets=task.job_facets if task else None,
            ),
            inputs=task.inputs if task else [],
            outputs=task.outputs if task else [],
            producer=_PRODUCER,
        )
        return self.emit(event)

    def complete_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        task: OperatorLineage,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        owners: list[str] | None,
        tags: list[str] | None,
        job_description: str | None,
        job_description_type: str | None = None,
        run_facets: dict[str, RunFacet] | None = None,
    ) -> RunEvent:
        """
        Emit openlineage event of type COMPLETE.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param job_description: description of the job
        :param job_description_type: MIME type of the description arg content
        :param tags: list of tags
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param task: metadata container with information extracted from operator
        :param owners: list of owners
        :param run_facets: additional run facets
        """
        run_facets = run_facets or {}
        if task:
            run_facets = {**task.run_facets, **run_facets}
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name,
                job_type=_JOB_TYPE_TASK,
                job_facets=task.job_facets,
                job_owners=owners,
                job_tags=tags,
                job_description=job_description,
                job_description_type=job_description_type,
            ),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        return self.emit(event)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        task: OperatorLineage,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        owners: list[str] | None,
        tags: list[str] | None,
        job_description: str | None,
        job_description_type: str | None = None,
        error: str | BaseException | None = None,
        run_facets: dict[str, RunFacet] | None = None,
    ) -> RunEvent:
        """
        Emit openlineage event of type FAIL.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        :param job_description: description of the job
        :param job_description_type: MIME type of the description arg content
        :param run_facets: custom run facets
        :param tags: list of tags
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param owners: list of owners
        :param error: error
        :param run_facets: additional run facets
        """
        run_facets = run_facets or {}
        if task:
            run_facets = {**task.run_facets, **run_facets}

        if error:
            stack_trace = None
            if isinstance(error, BaseException) and error.__traceback__:
                import traceback

                stack_trace = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=str(error), programmingLanguage="python", stackTrace=stack_trace
            )

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name,
                job_type=_JOB_TYPE_TASK,
                job_facets=task.job_facets,
                job_owners=owners,
                job_tags=tags,
                job_description=job_description,
                job_description_type=job_description_type,
            ),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        return self.emit(event)

    def dag_started(
        self,
        dag_id: str,
        logical_date: datetime,
        start_date: datetime,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        owners: list[str] | None,
        tags: list[str],
        run_facets: dict[str, RunFacet],
        clear_number: int,
        job_description: str | None,
        job_description_type: str | None = None,
        job_facets: dict[str, JobFacet] | None = None,  # Custom job facets
    ):
        try:
            event = RunEvent(
                eventType=RunState.START,
                eventTime=start_date.isoformat(),
                job=self._build_job(
                    job_name=dag_id,
                    job_type=_JOB_TYPE_DAG,
                    job_description=job_description,
                    job_description_type=job_description_type,
                    job_owners=owners,
                    job_facets=job_facets,
                    job_tags=tags,
                ),
                run=self._build_run(
                    run_id=self.build_dag_run_id(
                        dag_id=dag_id, logical_date=logical_date, clear_number=clear_number
                    ),
                    nominal_start_time=nominal_start_time,
                    nominal_end_time=nominal_end_time,
                    run_facets={**run_facets, **get_airflow_debug_facet()},
                ),
                inputs=[],
                outputs=[],
                producer=_PRODUCER,
            )
            self.emit(event)
        except BaseException:
            # Catch all exceptions to prevent ProcessPoolExecutor from silently swallowing them.
            # This ensures that any unexpected exceptions are logged for debugging purposes.
            # This part cannot be wrapped to deduplicate code, otherwise the method cannot be pickled in multiprocessing.
            self.log.warning("Failed to emit OpenLineage DAG started event: \n %s", traceback.format_exc())

    def dag_success(
        self,
        dag_id: str,
        run_id: str,
        end_date: datetime,
        logical_date: datetime,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        tags: list[str] | None,
        clear_number: int,
        dag_run_state: DagRunState,
        task_ids: list[str],
        owners: list[str] | None,
        run_facets: dict[str, RunFacet],
        job_description: str | None,
        job_description_type: str | None = None,
    ):
        try:
            event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=end_date.isoformat(),
                job=self._build_job(
                    job_name=dag_id,
                    job_type=_JOB_TYPE_DAG,
                    job_owners=owners,
                    job_tags=tags,
                    job_description=job_description,
                    job_description_type=job_description_type,
                ),
                run=self._build_run(
                    run_id=self.build_dag_run_id(
                        dag_id=dag_id, logical_date=logical_date, clear_number=clear_number
                    ),
                    nominal_start_time=nominal_start_time,
                    nominal_end_time=nominal_end_time,
                    run_facets={
                        **get_airflow_state_run_facet(dag_id, run_id, task_ids, dag_run_state),
                        **get_airflow_debug_facet(),
                        **run_facets,
                    },
                ),
                inputs=[],
                outputs=[],
                producer=_PRODUCER,
            )
            self.emit(event)
        except BaseException:
            # Catch all exceptions to prevent ProcessPoolExecutor from silently swallowing them.
            # This ensures that any unexpected exceptions are logged for debugging purposes.
            # This part cannot be wrapped to deduplicate code, otherwise the method cannot be pickled in multiprocessing.
            self.log.warning("Failed to emit OpenLineage DAG success event: \n %s", traceback.format_exc())

    def dag_failed(
        self,
        dag_id: str,
        run_id: str,
        end_date: datetime,
        logical_date: datetime,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        tags: list[str] | None,
        clear_number: int,
        dag_run_state: DagRunState,
        task_ids: list[str],
        owners: list[str] | None,
        msg: str,
        run_facets: dict[str, RunFacet],
        job_description: str | None,
        job_description_type: str | None = None,
    ):
        try:
            event = RunEvent(
                eventType=RunState.FAIL,
                eventTime=end_date.isoformat(),
                job=self._build_job(
                    job_name=dag_id,
                    job_type=_JOB_TYPE_DAG,
                    job_owners=owners,
                    job_tags=tags,
                    job_description=job_description,
                    job_description_type=job_description_type,
                ),
                run=self._build_run(
                    run_id=self.build_dag_run_id(
                        dag_id=dag_id, logical_date=logical_date, clear_number=clear_number
                    ),
                    nominal_start_time=nominal_start_time,
                    nominal_end_time=nominal_end_time,
                    run_facets={
                        "errorMessage": error_message_run.ErrorMessageRunFacet(
                            message=msg, programmingLanguage="python"
                        ),
                        **get_airflow_state_run_facet(dag_id, run_id, task_ids, dag_run_state),
                        **get_airflow_debug_facet(),
                        **run_facets,
                    },
                ),
                inputs=[],
                outputs=[],
                producer=_PRODUCER,
            )
            self.emit(event)
        except BaseException:
            # Catch all exceptions to prevent ProcessPoolExecutor from silently swallowing them.
            # This ensures that any unexpected exceptions are logged for debugging purposes.
            # This part cannot be wrapped to deduplicate code, otherwise the method cannot be pickled in multiprocessing.
            self.log.warning("Failed to emit OpenLineage DAG failed event: \n %s", traceback.format_exc())

    @staticmethod
    def _build_run(
        run_id: str,
        nominal_start_time: str | None = None,
        nominal_end_time: str | None = None,
        run_facets: dict[str, RunFacet] | None = None,
    ) -> Run:
        facets: dict[str, RunFacet] = {}
        if run_facets:
            facets.update(run_facets)
        if nominal_start_time:
            facets.update(
                {
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime=nominal_start_time,
                        nominalEndTime=nominal_end_time,
                        producer=_PRODUCER,
                    )
                }
            )
        facets.update(get_processing_engine_facet())

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_type: Literal["DAG", "TASK"],
        job_description: str | None = None,
        job_description_type: str | None = None,
        job_owners: list[str] | None = None,
        job_tags: list[str] | None = None,
        job_facets: dict[str, JobFacet] | None = None,
    ):
        facets: dict[str, JobFacet] = {}
        if job_facets:
            facets.update(job_facets)
        if job_description:
            facets.update(
                {
                    "documentation": documentation_job.DocumentationJobFacet(
                        description=job_description, contentType=job_description_type, producer=_PRODUCER
                    )
                }
            )
        if job_owners:
            facets.update(
                {
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[ownership_job.Owner(name=owner) for owner in sorted(job_owners)],
                        producer=_PRODUCER,
                    )
                }
            )
        if job_tags:
            facets.update(
                {
                    "tags": tags_job.TagsJobFacet(
                        tags=[
                            tags_job.TagsJobFacetFields(
                                key=tag,
                                value=tag,
                                source="AIRFLOW",
                            )
                            for tag in sorted(job_tags)
                        ],
                        producer=_PRODUCER,
                    )
                }
            )
        facets.update(
            {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType=job_type, integration="AIRFLOW", processingType="BATCH", producer=_PRODUCER
                )
            }
        )

        return Job(namespace=conf.namespace(), name=job_name, facets=facets)
