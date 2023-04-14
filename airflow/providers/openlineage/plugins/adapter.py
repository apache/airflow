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
import uuid
from typing import TYPE_CHECKING

import requests.exceptions

from airflow.providers.openlineage import version as OPENLINEAGE_PROVIDER_VERSION
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.utils.utils import OpenLineageRedactor
from airflow.utils.log.logging_mixin import LoggingMixin
from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.facet import (
    BaseFacet,
    DocumentationJobFacet,
    ErrorMessageRunFacet,
    NominalTimeRunFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    ParentRunFacet,
    ProcessingEngineRunFacet,
    SourceCodeLocationJobFacet,
)
from openlineage.client.run import Job, Run, RunEvent, RunState

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.utils.log.secrets_masker import SecretsMasker

_DAG_DEFAULT_NAMESPACE = "default"

_DAG_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", _DAG_DEFAULT_NAMESPACE)

_PRODUCER = f"https://github.com/apache/airflow/tree/providers-openlineage/" f"{OPENLINEAGE_PROVIDER_VERSION}"

set_producer(_PRODUCER)


class OpenLineageAdapter(LoggingMixin):
    """
    Adapter for translating Airflow metadata to OpenLineage events,
    instead of directly creating them from Airflow code.
    """

    def __init__(self, client: OpenLineageClient | None = None, secrets_masker: SecretsMasker | None = None):
        super().__init__()
        self._client = client or OpenLineageClient.from_environment()
        if not secrets_masker:
            from airflow.utils.log.secrets_masker import _secrets_masker

            secrets_masker = _secrets_masker()
        self._redacter = OpenLineageRedactor.from_masker(secrets_masker)

    def get_or_create_openlineage_client(self) -> OpenLineageClient:
        if not self._client:
            self._client = OpenLineageClient.from_environment()
        return self._client

    def build_dag_run_id(self, dag_id, dag_run_id):
        return str(uuid.uuid3(uuid.NAMESPACE_URL, f"{_DAG_NAMESPACE}.{dag_id}.{dag_run_id}"))

    @staticmethod
    def build_task_instance_run_id(task_id, execution_date, try_number):
        return str(
            uuid.uuid3(
                uuid.NAMESPACE_URL,
                f"{_DAG_NAMESPACE}.{task_id}.{execution_date}.{try_number}",
            )
        )

    def emit(self, event: RunEvent):
        event = self._redacter.redact(event, max_depth=20)
        try:
            return self._client.emit(event)
        except requests.exceptions.RequestException:
            self.log.exception(f"Failed to emit OpenLineage event of id {event.run.runId}")

    def start_task(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_job_name: str | None,
        parent_run_id: str | None,
        code_location: str | None,
        nominal_start_time: str,
        nominal_end_time: str,
        owners: list[str],
        task: OperatorLineage | None,
        run_facets: dict[str, type[BaseFacet]] | None = None,  # Custom run facets
    ):
        """
        Emits openlineage event of type START

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
        run_facets["processing_engine"] = processing_engine_version_facet  # type: ignore
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id,
                parent_job_name,
                parent_run_id,
                job_name,
                nominal_start_time,
                nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name=job_name,
                job_description=job_description,
                code_location=code_location,
                owners=owners,
                job_facets=task.job_facets if task else None,
            ),
            inputs=task.inputs if task else [],
            outputs=task.outputs if task else [],
            producer=_PRODUCER,
        )
        self.emit(event)

    def complete_task(self, run_id: str, job_name: str, end_time: str, task: OperatorLineage):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(run_id, run_facets=task.run_facets),
            job=self._build_job(job_name, job_facets=task.job_facets),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        self.emit(event)

    def fail_task(self, run_id: str, job_name: str, end_time: str, task: OperatorLineage):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(run_id, run_facets=task.run_facets),
            job=self._build_job(job_name),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        self.emit(event)

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
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
            run=self._build_run(
                run_id=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id),
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
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
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
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
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
        parent_job_name: str | None = None,
        parent_run_id: str | None = None,
        job_name: str | None = None,
        nominal_start_time: str | None = None,
        nominal_end_time: str | None = None,
        run_facets: dict[str, BaseFacet] | None = None,
    ) -> Run:
        facets = {}
        if nominal_start_time:
            facets.update({"nominalTime": NominalTimeRunFacet(nominal_start_time, nominal_end_time)})
        if parent_run_id:
            parent_run_facet = ParentRunFacet.create(
                runId=parent_run_id,
                namespace=_DAG_NAMESPACE,
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
        job_description: str | None = None,
        code_location: str | None = None,
        owners: list[str] | None = None,
        job_facets: dict[str, BaseFacet] | None = None,
    ):
        facets = {}

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

        return Job(_DAG_NAMESPACE, job_name, facets)
