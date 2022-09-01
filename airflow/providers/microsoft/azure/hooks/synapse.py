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

import time
from typing import Any, Dict, Optional, Set, Union

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.synapse.spark import SparkClient
from azure.synapse.spark.models import SparkBatchJobOptions

from airflow.exceptions import AirflowTaskTimeout
from airflow.hooks.base import BaseHook

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class AzureSynapseSparkBatchRunStatus:
    """Azure Synapse Spark Job operation statuses."""

    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    RUNNING = 'running'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    KILLED = 'killed'
    SUCCESS = 'success'

    TERMINAL_STATUSES = {SUCCESS, DEAD, KILLED, ERROR}


class AzureSynapseHook(BaseHook):
    """
    A hook to interact with Azure Synapse.
    :param azure_synapse_conn_id: The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    :param spark_pool: The Apache Spark pool used to submit the job
    """

    conn_type: str = 'azure_synapse'
    conn_name_attr: str = 'azure_synapse_conn_id'
    default_conn_name: str = 'azure_synapse_default'
    hook_name: str = 'Azure Synapse'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_synapse__tenantId": StringField(
                lazy_gettext('Tenant ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure_synapse__subscriptionId": StringField(
                lazy_gettext('Subscription ID'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'extra'],
            "relabeling": {'login': 'Client ID', 'password': 'Secret', 'host': 'Synapse Workspace URL'},
        }

    def __init__(self, azure_synapse_conn_id: str = default_conn_name, spark_pool: str = ''):
        self.job_id: Optional[int] = None
        self._conn: Optional[SparkClient] = None
        self.conn_id = azure_synapse_conn_id
        self.spark_pool = spark_pool
        super().__init__()

    def get_conn(self) -> SparkClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get('extra__azure_synapse__tenantId')
        spark_pool = self.spark_pool
        livy_api_version = "2022-02-22-preview"

        try:
            subscription_id = conn.extra_dejson['extra__azure_synapse__subscriptionId']
        except KeyError:
            raise ValueError("A Subscription ID is required to connect to Azure Synapse.")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()

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
        job_id: Optional[int],
        expected_statuses: Union[str, Set[str]],
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a job run to match an expected status.

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
            self.log.info("Sleeping for %s seconds", str(check_interval))
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
