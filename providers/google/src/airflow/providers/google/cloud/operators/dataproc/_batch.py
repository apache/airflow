#
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
"""DataprocCreateBatchOperator with lazy-loaded heavy deps to avoid DagBag timeout (#62373)."""

from __future__ import annotations

import re
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.google.cloud.links.dataproc import DATAPROC_BATCH_LINK, DataprocBatchLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core import operation
    from google.api_core.retry import Retry
    from google.api_core.retry_async import AsyncRetry
    from google.cloud.dataproc_v1 import Batch

    from airflow.providers.common.compat.sdk import Context


class DataprocCreateBatchOperator(GoogleCloudBaseOperator):
    """
    Create a batch workload.

    Lazy-loads google.cloud.dataproc_v1 and DataprocHook to avoid slow DAG parsing (#62373).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "batch",
        "batch_id",
        "region",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocBatchLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str = PROVIDE_PROJECT_ID,
        batch: dict | Batch,
        batch_id: str | None = None,
        request_id: str | None = None,
        num_retries_if_resource_is_not_ready: int = 0,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        result_retry: AsyncRetry | _MethodDefault | Retry = DEFAULT,
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 5,
        openlineage_inject_parent_job_info: bool = conf.getboolean(
            "openlineage", "spark_inject_parent_job_info", fallback=False
        ),
        openlineage_inject_transport_info: bool = conf.getboolean(
            "openlineage", "spark_inject_transport_info", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.region = region
        self.project_id = project_id
        self.batch = batch
        self.batch_id = batch_id
        self.request_id = request_id
        self.num_retries_if_resource_is_not_ready = num_retries_if_resource_is_not_ready
        self.retry = retry
        self.result_retry = result_retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.operation: operation.Operation | None = None
        self.asynchronous = asynchronous
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds
        self.openlineage_inject_parent_job_info = openlineage_inject_parent_job_info
        self.openlineage_inject_transport_info = openlineage_inject_transport_info

    def execute(self, context: Context):
        from google.api_core.exceptions import AlreadyExists
        from google.cloud.dataproc_v1 import Batch

        from airflow.providers.google.cloud.triggers.dataproc import DataprocBatchTrigger

        if self.asynchronous and self.deferrable:
            raise AirflowException(
                "Both asynchronous and deferrable parameters were passed. Please, provide only one."
            )

        batch_id: str = ""
        if self.batch_id:
            batch_id = self.batch_id
            self.log.info("Starting batch %s", batch_id)
            DataprocBatchLink.persist(
                context=context,
                project_id=self.project_id,
                region=self.region,
                batch_id=self.batch_id,
            )
        else:
            self.log.info("Starting batch. The batch ID will be generated since it was not provided.")

        if self.openlineage_inject_parent_job_info or self.openlineage_inject_transport_info:
            self.log.info("Automatic injection of OpenLineage information into Spark properties is enabled.")
            self._inject_openlineage_properties_into_dataproc_batch(context)

        self.__update_batch_labels()

        try:
            self.operation = self.hook.create_batch(
                region=self.region,
                project_id=self.project_id,
                batch=self.batch,
                batch_id=self.batch_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Batch with given id already exists.")
            self.log.info("Attaching to the job %s if it is still running.", batch_id)
        else:
            if self.operation and self.operation.metadata:
                batch_id = self.operation.metadata.batch.split("/")[-1]
            else:
                raise AirflowException("Operation metadata is not available.")
            self.log.info("The batch %s was created.", batch_id)

        DataprocBatchLink.persist(
            context=context,
            project_id=self.project_id,
            region=self.region,
            batch_id=batch_id,
        )

        if self.asynchronous:
            batch = self.hook.get_batch(
                batch_id=batch_id,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("The batch %s was created asynchronously. Exiting.", batch_id)
            return Batch.to_dict(batch)

        if self.deferrable:
            self.defer(
                trigger=DataprocBatchTrigger(
                    batch_id=batch_id,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )

        self.log.info("Waiting for the completion of batch job %s", batch_id)
        batch = self.hook.wait_for_batch(
            batch_id=batch_id,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if self.num_retries_if_resource_is_not_ready and self.hook.check_error_for_resource_is_not_ready_msg(
            batch.state_message
        ):
            attempt = self.num_retries_if_resource_is_not_ready
            while attempt > 0:
                attempt -= 1
                batch, batch_id = self.retry_batch_creation(batch_id)
                if not self.hook.check_error_for_resource_is_not_ready_msg(batch.state_message):
                    break

        self.handle_batch_status(context, batch.state.name, batch_id, batch.state_message)
        return Batch.to_dict(batch)

    @cached_property
    def hook(self):
        from airflow.providers.google.cloud.hooks.dataproc import DataprocHook

        return DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

    def execute_complete(self, context, event=None) -> None:
        if event is None:
            raise AirflowException("Batch failed.")
        state = event["batch_state"]
        batch_id = event["batch_id"]
        self.handle_batch_status(context, state, batch_id, state_message=event["batch_state_message"])

    def on_kill(self):
        if self.operation:
            self.operation.cancel()

    def handle_batch_status(
        self, context: Context, state: str, batch_id: str, state_message: str | None = None
    ) -> None:
        from google.cloud.dataproc_v1 import Batch

        link = DATAPROC_BATCH_LINK.format(region=self.region, project_id=self.project_id, batch_id=batch_id)
        if state == Batch.State.FAILED.name:  # type: ignore
            raise AirflowException(
                f"Batch job {batch_id} failed with error: {state_message}.\nDriver logs: {link}"
            )
        if state in (Batch.State.CANCELLED.name, Batch.State.CANCELLING.name):  # type: ignore
            raise AirflowException(f"Batch job {batch_id} was cancelled.\nDriver logs: {link}")
        if state == Batch.State.STATE_UNSPECIFIED.name:  # type: ignore
            raise AirflowException(f"Batch job {batch_id} unspecified.\nDriver logs: {link}")
        self.log.info("Batch job %s completed.\nDriver logs: %s", batch_id, link)

    def retry_batch_creation(
        self,
        previous_batch_id: str,
    ):
        from google.api_core.exceptions import AlreadyExists

        self.log.info("Retrying creation process for batch_id %s", self.batch_id)
        self.log.info("Deleting previous failed Batch")
        self.hook.delete_batch(
            batch_id=previous_batch_id,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Starting a new creation for batch_id %s", self.batch_id)
        try:
            self.operation = self.hook.create_batch(
                region=self.region,
                project_id=self.project_id,
                batch=self.batch,
                batch_id=self.batch_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Batch with given id already exists.")
            self.log.info("Attaching to the job %s if it is still running.", self.batch_id)
        else:
            if self.operation and self.operation.metadata:
                batch_id = self.operation.metadata.batch.split("/")[-1]
                self.log.info("The batch %s was created.", batch_id)
            else:
                raise AirflowException("Operation metadata is not available.")

        self.log.info("Waiting for the completion of batch job %s", batch_id)
        batch = self.hook.wait_for_batch(
            batch_id=batch_id,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return batch, batch_id

    def _inject_openlineage_properties_into_dataproc_batch(self, context: Context) -> None:
        try:
            from airflow.providers.google.cloud.openlineage.utils import (
                inject_openlineage_properties_into_dataproc_batch,
            )

            self.batch = inject_openlineage_properties_into_dataproc_batch(
                batch=self.batch,
                context=context,
                inject_parent_job_info=self.openlineage_inject_parent_job_info,
                inject_transport_info=self.openlineage_inject_transport_info,
            )
        except Exception as e:
            self.log.warning(
                "An error occurred while trying to inject OpenLineage information. "
                "Dataproc batch has not been modified by OpenLineage.",
                exc_info=e,
            )

    def __update_batch_labels(self):
        from google.cloud.dataproc_v1 import Batch

        dag_id = re.sub(r"[.\s]", "_", self.dag_id.lower())
        task_id = re.sub(r"[.\s]", "_", self.task_id.lower())

        labels_regex = re.compile(r"^[a-z][\w-]{0,62}$")
        if not labels_regex.match(dag_id) or not labels_regex.match(task_id):
            return

        labels_limit = 32
        new_labels = {"airflow-dag-id": dag_id, "airflow-task-id": task_id}

        if self._dag:
            dag_display_name = re.sub(r"[.\s]", "_", self._dag.dag_display_name.lower())
            if labels_regex.match(dag_id):
                new_labels["airflow-dag-display-name"] = dag_display_name

        if isinstance(self.batch, Batch):
            if len(self.batch.labels) + len(new_labels) <= labels_limit:
                self.batch.labels.update(new_labels)
        elif "labels" not in self.batch:
            self.batch["labels"] = new_labels
        elif isinstance(self.batch.get("labels"), dict):
            if len(self.batch["labels"]) + len(new_labels) <= labels_limit:
                self.batch["labels"].update(new_labels)
