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

import time
import warnings
from typing import TYPE_CHECKING, Any, Union

from azure.core.exceptions import ServiceRequestError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.spark import SparkClient

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowTaskTimeout
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.synapse.artifacts.models import CreateRunResponse, PipelineRun
    from azure.synapse.spark.models import SparkBatchJobOptions

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class AzureSynapseSparkBatchRunStatus:
    """Azure Synapse Spark Job operation statuses."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    RUNNING = "running"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"

    TERMINAL_STATUSES = {SUCCESS, DEAD, KILLED, ERROR}


class AzureSynapseHook(BaseHook):
    """
    A hook to interact with Azure Synapse.

    :param azure_synapse_conn_id: The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    :param spark_pool: The Apache Spark pool used to submit the job
    """

    conn_type: str = "azure_synapse"
    conn_name_attr: str = "azure_synapse_conn_id"
    default_conn_name: str = "azure_synapse_default"
    hook_name: str = "Azure Synapse"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "subscriptionId": StringField(lazy_gettext("Subscription ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Secret",
                "host": "Synapse Workspace URL",
            },
        }

    def __init__(self, azure_synapse_conn_id: str = default_conn_name, spark_pool: str = ""):
        self.job_id: int | None = None
        self._conn: SparkClient | None = None
        self.conn_id = azure_synapse_conn_id
        self.spark_pool = spark_pool
        super().__init__()

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    def get_conn(self) -> SparkClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")
        spark_pool = self.spark_pool
        livy_api_version = "2022-02-22-preview"

        subscription_id = self._get_field(extras, "subscriptionId")
        if not subscription_id:
            raise ValueError("A Subscription ID is required to connect to Azure Synapse.")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
            workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        self._conn = self._create_client(credential, conn.host, spark_pool, livy_api_version, subscription_id)

        return self._conn

    @staticmethod
    def _create_client(credential: Credentials, host, spark_pool, livy_api_version, subscription_id: str):
        return SparkClient(
            credential=credential,
            endpoint=host,
            spark_pool_name=spark_pool,
            livy_api_version=livy_api_version,
            subscription_id=subscription_id,
        )

    def run_spark_job(
        self,
        payload: SparkBatchJobOptions,
    ):
        """
        Run a job in an Apache Spark pool.

        :param payload: Livy compatible payload which represents the spark job that a user wants to submit.
        """
        job = self.get_conn().spark_batch.create_spark_batch_job(payload)
        self.job_id = job.id
        return job

    def get_job_run_status(self):
        """Get the job run status."""
        job_run_status = self.get_conn().spark_batch.get_spark_batch_job(batch_id=self.job_id).state
        return job_run_status

    def wait_for_job_run_status(
        self,
        job_id: int | None,
        expected_statuses: str | set[str],
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Wait for a job run to match an expected status.

        :param job_id: The job run identifier.
        :param expected_statuses: The desired status(es) to check against a job run's current status.
        :param check_interval: Time in seconds to check on a job run's status.
        :param timeout: Time in seconds to wait for a job to reach a terminal status or the expected
            status.

        """
        job_run_status = self.get_job_run_status()
        start_time = time.monotonic()

        while (
            job_run_status not in AzureSynapseSparkBatchRunStatus.TERMINAL_STATUSES
            and job_run_status not in expected_statuses
        ):
            # Check if the job-run duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise AirflowTaskTimeout(
                    f"Job {job_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the job run based on the ``check_interval`` configured.
            self.log.info("Sleeping for %s seconds", check_interval)
            time.sleep(check_interval)

            job_run_status = self.get_job_run_status()
            self.log.info("Current spark job run status is %s", job_run_status)

        return job_run_status in expected_statuses

    def cancel_job_run(
        self,
        job_id: int,
    ) -> None:
        """
        Cancel the spark job run.

        :param job_id: The synapse spark job identifier.
        """
        self.get_conn().spark_batch.cancel_spark_batch_job(job_id)


class AzureSynapsePipelineRunStatus:
    """Azure Synapse pipeline operation statuses."""

    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELING = "Canceling"
    CANCELLED = "Cancelled"
    TERMINAL_STATUSES = {CANCELLED, FAILED, SUCCEEDED}
    INTERMEDIATE_STATES = {QUEUED, IN_PROGRESS, CANCELING}
    FAILURE_STATES = {FAILED, CANCELLED}


class AzureSynapsePipelineRunException(AirflowException):
    """An exception that indicates a pipeline run failed to complete."""


class BaseAzureSynapseHook(BaseHook):
    """
    A base hook class to create session and connection to Azure Synapse using connection id.

    :param azure_synapse_conn_id: The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    """

    conn_type: str = "azure_synapse"
    conn_name_attr: str = "azure_synapse_conn_id"
    default_conn_name: str = "azure_synapse_default"
    hook_name: str = "Azure Synapse"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "subscriptionId": StringField(lazy_gettext("Subscription ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Secret",
                "host": "Synapse Workspace URL",
            },
        }

    def __init__(self, azure_synapse_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = azure_synapse_conn_id

    def _get_field(self, extras: dict, field_name: str) -> str:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )


class AzureSynapsePipelineHook(BaseAzureSynapseHook):
    """
    A hook to interact with Azure Synapse Pipeline.

    :param azure_synapse_conn_id: The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    :param azure_synapse_workspace_dev_endpoint: The Azure Synapse Workspace development endpoint.
    """

    default_conn_name: str = "azure_synapse_connection"

    def __init__(
        self,
        azure_synapse_workspace_dev_endpoint: str,
        azure_synapse_conn_id: str = default_conn_name,
        **kwargs,
    ):
        # Handling deprecation of "default_conn_name"
        if azure_synapse_conn_id == self.default_conn_name:
            warnings.warn(
                "The usage of `default_conn_name=azure_synapse_connection` is deprecated and will be removed in future. Please update your code to use the new default connection name: `default_conn_name=azure_synapse_default`. ",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        self._conn: ArtifactsClient | None = None
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        super().__init__(azure_synapse_conn_id=azure_synapse_conn_id, **kwargs)

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    def get_conn(self) -> ArtifactsClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        credential: Credentials
        if not conn.login or not conn.password:
            managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
            workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")

            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )
        else:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )

        self._conn = self._create_client(credential, self.azure_synapse_workspace_dev_endpoint)

        if self._conn is not None:
            return self._conn
        else:
            raise ValueError("Failed to create ArtifactsClient")

    @staticmethod
    def _create_client(credential: Credentials, endpoint: str) -> ArtifactsClient:
        return ArtifactsClient(credential=credential, endpoint=endpoint)

    def run_pipeline(self, pipeline_name: str, **config: Any) -> CreateRunResponse:
        """
        Run a Synapse pipeline.

        :param pipeline_name: The pipeline name.
        :param config: Extra parameters for the Synapse Artifact Client.
        :return: The pipeline run Id.
        """
        return self.get_conn().pipeline.create_pipeline_run(pipeline_name, **config)

    def get_pipeline_run(self, run_id: str) -> PipelineRun:
        """
        Get the pipeline run.

        :param run_id: The pipeline run identifier.
        :return: The pipeline run.
        """
        return self.get_conn().pipeline_run.get_pipeline_run(run_id=run_id)

    def get_pipeline_run_status(self, run_id: str) -> str:
        """
        Get a pipeline run's current status.

        :param run_id: The pipeline run identifier.

        :return: The status of the pipeline run.
        """
        pipeline_run_status = self.get_pipeline_run(
            run_id=run_id,
        ).status

        return str(pipeline_run_status)

    def refresh_conn(self) -> ArtifactsClient:
        self._conn = None
        return self.get_conn()

    def wait_for_pipeline_run_status(
        self,
        run_id: str,
        expected_statuses: str | set[str],
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Wait for a pipeline run to match an expected status.

        :param run_id: The pipeline run identifier.
        :param expected_statuses: The desired status(es) to check against a pipeline run's current status.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.

        :return: Boolean indicating if the pipeline run has reached the ``expected_status``.
        """
        pipeline_run_status = self.get_pipeline_run_status(run_id=run_id)
        executed_after_token_refresh = True
        start_time = time.monotonic()

        while (
            pipeline_run_status not in AzureSynapsePipelineRunStatus.TERMINAL_STATUSES
            and pipeline_run_status not in expected_statuses
        ):
            if start_time + timeout < time.monotonic():
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {run_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the pipeline run based on the ``check_interval`` configured.
            time.sleep(check_interval)

            try:
                pipeline_run_status = self.get_pipeline_run_status(run_id=run_id)
                executed_after_token_refresh = True
            except ServiceRequestError:
                if executed_after_token_refresh:
                    self.refresh_conn()
                else:
                    raise

        return pipeline_run_status in expected_statuses

    def cancel_run_pipeline(self, run_id: str) -> None:
        """
        Cancel the pipeline run.

        :param run_id: The pipeline run identifier.
        """
        self.get_conn().pipeline_run.cancel_pipeline_run(run_id)
