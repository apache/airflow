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

import uuid
from contextlib import ExitStack
from typing import TYPE_CHECKING

import yaml
from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.facet import (
    BaseFacet,
    DocumentationJobFacet,
    ErrorMessageRunFacet,
    JobTypeJobFacet,
    NominalTimeRunFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    ParentRunFacet,
    ProcessingEngineRunFacet,
    SourceCodeLocationJobFacet,
)
from openlineage.client.run import Job, Run, RunEvent, RunState

from airflow.providers.openlineage import __version__ as OPENLINEAGE_PROVIDER_VERSION, conf
from airflow.providers.openlineage.utils.utils import OpenLineageRedactor
from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.log.secrets_masker import SecretsMasker

_PRODUCER = f"https://github.com/apache/airflow/tree/providers-openlineage/{OPENLINEAGE_PROVIDER_VERSION}"

set_producer(_PRODUCER)

# https://openlineage.io/docs/spec/facets/job-facets/job-type
# They must be set after the `set_producer(_PRODUCER)`
# otherwise the `JobTypeJobFacet._producer` will be set with the default value
_JOB_TYPE_DAG = JobTypeJobFacet(jobType="DAG", integration="AIRFLOW", processingType="BATCH")
_JOB_TYPE_TASK = JobTypeJobFacet(jobType="TASK", integration="AIRFLOW", processingType="BATCH")


class OpenLineageAdapter(LoggingMixin):
    """Translate Airflow metadata to OpenLineage events instead of creating them from Airflow code."""

    def __init__(self, client: OpenLineageClient | None = None, secrets_masker: SecretsMasker | None = None):
        super().__init__()
        self._client = client
        if not secrets_masker:
            from airflow.utils.log.secrets_masker import _secrets_masker

            secrets_masker = _secrets_masker()
        self._redacter = OpenLineageRedactor.from_masker(secrets_masker)

    def get_or_create_openlineage_client(self) -> OpenLineageClient:
        if not self._client:
            config = self.get_openlineage_config()
            if config:
                self.log.debug(
                    "OpenLineage configuration found. Transport type: `%s`",
                    config.get("type", "no type provided"),
                )
                self._client = OpenLineageClient.from_dict(config=config)
            else:
                self.log.debug(
                    "OpenLineage configuration not found directly in Airflow. "
                    "Looking for legacy environment configuration. "
                )
                self._client = OpenLineageClient.from_environment()
        return self._client

    def get_openlineage_config(self) -> dict | None:
        # First, try to read from YAML file
        openlineage_config_path = conf.config_path(check_legacy_env_var=False)
        if openlineage_config_path:
            config = self._read_yaml_config(openlineage_config_path)
            if config:
                return config.get("transport", None)
            self.log.debug("OpenLineage config file is empty: `%s`", openlineage_config_path)
        else:
            self.log.debug("OpenLineage config_path configuration not found.")

        # Second, try to get transport config
        transport_config = conf.transport()
        if not transport_config:
            self.log.debug("OpenLineage transport configuration not found.")
            return None
        return transport_config

    @staticmethod
    def _read_yaml_config(path: str) -> dict | None:
        with open(path) as config_file:
            return yaml.safe_load(config_file)

    @staticmethod
    def build_dag_run_id(dag_id, dag_run_id):
        return str(uuid.uuid3(uuid.NAMESPACE_URL, f"{conf.namespace()}.{dag_id}.{dag_run_id}"))

    @staticmethod
    def build_task_instance_run_id(dag_id, task_id, execution_date, try_number):
        return str(
            uuid.uuid3(
                uuid.NAMESPACE_URL,
                f"{conf.namespace()}.{dag_id}.{task_id}.{execution_date}.{try_number}",
            )
        )

    def emit(self, event: RunEvent):
        """Emit OpenLineage event.

        :param event: Event to be emitted.
        :return: Redacted Event.
        """
        if not self._client:
            self._client = self.get_or_create_openlineage_client()
        redacted_event: RunEvent = self._redacter.redact(event, max_depth=20)  # type: ignore[assignment]
        event_type = event.eventType.value.lower()
        transport_type = f"{self._client.transport.kind}".lower()

        try:
            with ExitStack() as stack:
                stack.enter_context(Stats.timer(f"ol.emit.attempts.{event_type}.{transport_type}"))
                stack.enter_context(Stats.timer("ol.emit.attempts"))
                self._client.emit(redacted_event)
                self.log.debug("Successfully emitted OpenLineage event of id %s", event.run.runId)
        except Exception as e:
            Stats.incr("ol.emit.failed")
            self.log.warning("Failed to emit OpenLineage event of id %s", event.run.runId)
            self.log.debug("OpenLineage emission failure: %s", e)

        return redacted_event

    def start_task(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_job_name: str | None,
        parent_run_id: str | None,
        code_location: str | None,
        nominal_start_time: str | None,
        nominal_end_time: str | None,
        owners: list[str],
        task: OperatorLineage | None,
        run_facets: dict[str, BaseFacet] | None = None,  # Custom run facets
    ) -> RunEvent:
        """
        Emit openlineage event of type START.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task in dag
        :param job_description: user provided description of job
        :param event_time:
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param code_location: file path or URL of DAG file
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param owners: list of owners of DAG
        :param task: metadata container with information extracted from operator
        :param run_facets: custom run facets
        """
        from airflow.version import version as AIRFLOW_VERSION

        processing_engine_version_facet = ProcessingEngineRunFacet(
            version=AIRFLOW_VERSION,
            name="Airflow",
            openlineageAdapterVersion=OPENLINEAGE_PROVIDER_VERSION,
        )

        if not run_facets:
            run_facets = {}
        if task:
            run_facets = {**task.run_facets, **run_facets}
        run_facets["processing_engine"] = processing_engine_version_facet  # type: ignore
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id=run_id,
                job_name=job_name,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name=job_name,
                job_type=_JOB_TYPE_TASK,
                job_description=job_description,
                code_location=code_location,
                owners=owners,
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
        parent_job_name: str | None,
        parent_run_id: str | None,
        end_time: str,
        task: OperatorLineage,
    ) -> RunEvent:
        """
        Emit openlineage event of type COMPLETE.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                job_name=job_name,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                run_facets=task.run_facets,
            ),
            job=self._build_job(job_name, job_type=_JOB_TYPE_TASK, job_facets=task.job_facets),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        return self.emit(event)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        parent_job_name: str | None,
        parent_run_id: str | None,
        end_time: str,
        task: OperatorLineage,
    ) -> RunEvent:
        """
        Emit openlineage event of type FAIL.

        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                job_name=job_name,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                run_facets=task.run_facets,
            ),
            job=self._build_job(job_name, job_type=_JOB_TYPE_TASK, job_facets=task.job_facets),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        return self.emit(event)

    def dag_started(
        self,
        dag_run: DagRun,
        msg: str,
        nominal_start_time: str,
        nominal_end_time: str,
    ):
        event = RunEvent(
            eventType=RunState.START,
            eventTime=dag_run.start_date.isoformat(),
            job=self._build_job(job_name=dag_run.dag_id, job_type=_JOB_TYPE_DAG),
            run=self._build_run(
                run_id=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id),
                job_name=dag_run.dag_id,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
            ),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    def dag_success(self, dag_run: DagRun, msg: str):
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=dag_run.end_date.isoformat(),
            job=self._build_job(job_name=dag_run.dag_id, job_type=_JOB_TYPE_DAG),
            run=Run(runId=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id)),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    def dag_failed(self, dag_run: DagRun, msg: str):
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=dag_run.end_date.isoformat(),
            job=self._build_job(job_name=dag_run.dag_id, job_type=_JOB_TYPE_DAG),
            run=Run(
                runId=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id),
                facets={"errorMessage": ErrorMessageRunFacet(message=msg, programmingLanguage="python")},
            ),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    @staticmethod
    def _build_run(
        run_id: str,
        job_name: str,
        parent_job_name: str | None = None,
        parent_run_id: str | None = None,
        nominal_start_time: str | None = None,
        nominal_end_time: str | None = None,
        run_facets: dict[str, BaseFacet] | None = None,
    ) -> Run:
        facets: dict[str, BaseFacet] = {}
        if nominal_start_time:
            facets.update({"nominalTime": NominalTimeRunFacet(nominal_start_time, nominal_end_time)})
        if parent_run_id:
            parent_run_facet = ParentRunFacet.create(
                runId=parent_run_id,
                namespace=conf.namespace(),
                name=parent_job_name or job_name,
            )
            facets.update(
                {
                    "parent": parent_run_facet,
                    "parentRun": parent_run_facet,  # Keep sending this for the backward compatibility
                }
            )

        if run_facets:
            facets.update(run_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_type: JobTypeJobFacet,
        job_description: str | None = None,
        code_location: str | None = None,
        owners: list[str] | None = None,
        job_facets: dict[str, BaseFacet] | None = None,
    ):
        facets: dict[str, BaseFacet] = {}

        if job_description:
            facets.update({"documentation": DocumentationJobFacet(description=job_description)})
        if code_location:
            facets.update({"sourceCodeLocation": SourceCodeLocationJobFacet("", url=code_location)})
        if owners:
            facets.update(
                {
                    "ownership": OwnershipJobFacet(
                        owners=[OwnershipJobFacetOwners(name=owner) for owner in owners]
                    )
                }
            )
        if job_facets:
            facets = {**facets, **job_facets}

        facets.update({"jobType": job_type})

        return Job(conf.namespace(), job_name, facets)
