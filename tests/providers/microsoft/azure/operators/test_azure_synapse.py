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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.models import Connection
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
SUBSCRIPTION_ID = "my-subscription-id"
TASK_ID = "run_spark_op"
AZURE_SYNAPSE_CONN_ID = "azure_synapse_test"
CONN_EXTRAS = {
    "synapse__subscriptionId": SUBSCRIPTION_ID,
    "synapse__tenantId": "my-tenant-id",
    "synapse__spark_pool": "my-spark-pool",
}
JOB_RUN_RESPONSE = {"id": 123}


class TestAzureSynapseRunSparkBatchOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "task_id": TASK_ID,
            "azure_synapse_conn_id": AZURE_SYNAPSE_CONN_ID,
            "payload": {},
            "check_interval": 1,
            "timeout": 3,
        }

        create_mock_connection(
            Connection(
                conn_id=AZURE_SYNAPSE_CONN_ID,
                conn_type="azure_synapse",
                host="https://synapsetest.net",
                login="client-id",
                password="client-secret",
                extra=CONN_EXTRAS,
            )
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    def test_azure_synapse_run_spark_batch_operator_success(
        self, mock_run_spark_job, mock_get_job_run_status
    ):
        mock_get_job_run_status.return_value = "success"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        op.execute(context=self.mock_context)
        assert op.job_id == JOB_RUN_RESPONSE["id"]

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    def test_azure_synapse_run_spark_batch_operator_error(self, mock_run_spark_job, mock_get_job_run_status):
        mock_get_job_run_status.return_value = "error"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        with pytest.raises(
            Exception,
            match=f"Job run {JOB_RUN_RESPONSE['id']} has failed or has been cancelled.",
        ):
            op.execute(context=self.mock_context)

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.cancel_job_run")
    def test_azure_synapse_run_spark_batch_operator_on_kill(
        self, mock_cancel_job_run, mock_run_spark_job, mock_get_job_run_status
    ):
        mock_get_job_run_status.return_value = "success"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        op.execute(context=self.mock_context)
        op.on_kill()
        mock_cancel_job_run.assert_called_once_with(job_id=JOB_RUN_RESPONSE["id"])
