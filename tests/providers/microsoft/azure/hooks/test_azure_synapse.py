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


import json
from typing import Type
from unittest.mock import MagicMock, patch

import pytest
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.synapse.spark import SparkClient
from pytest import fixture

from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.synapse import AzureSynapseHook, AzureSynapseSparkBatchRunStatus
from airflow.utils import db

DEFAULT_SPARK_POOL = "defaultSparkPool"

DEFAULT_SYNAPSE = "defaultSynapse"
SYNAPSE = "testSynapse"

DEFAULT_CONNECTION_CLIENT_SECRET = "azure_synapse_test_client_secret"
DEFAULT_CONNECTION_DEFAULT_CREDENTIAL = "azure_synapse_test_default_credential"

MODEL = object()
NAME = "testName"
ID = "testId"
JOB_ID = 1


def setup_module():
    connection_client_secret = Connection(
        conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
        conn_type="azure_synapse",
        host="https://testsynapse.dev.azuresynapse.net",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "extra__azure_synapse__tenantId": "tenantId",
                "extra__azure_synapse__subscriptionId": "subscriptionId",
            }
        ),
    )
    connection_default_credential = Connection(
        conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
        conn_type="azure_synapse",
        host="https://testsynapse.dev.azuresynapse.net",
        extra=json.dumps(
            {
                "extra__azure_synapse__subscriptionId": "subscriptionId",
            }
        ),
    )
    connection_missing_subscription_id = Connection(
        conn_id="azure_synapse_missing_subscription_id",
        conn_type="azure_synapse",
        host="https://testsynapse.dev.azuresynapse.net",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "extra__azure_synapse__tenantId": "tenantId",
            }
        ),
    )
    connection_missing_tenant_id = Connection(
        conn_id="azure_synapse_missing_tenant_id",
        conn_type="azure_synapse",
        host="https://testsynapse.dev.azuresynapse.net",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "extra__azure_synapse__subscriptionId": "subscriptionId",
            }
        ),
    )

    db.merge_conn(connection_client_secret)
    db.merge_conn(connection_default_credential)
    db.merge_conn(connection_missing_subscription_id)
    db.merge_conn(connection_missing_tenant_id)


@fixture
def hook():
    client = AzureSynapseHook(azure_synapse_conn_id=DEFAULT_CONNECTION_CLIENT_SECRET)
    client._conn = MagicMock(spec=["spark_batch"])

    return client


@pytest.mark.parametrize(
    ("connection_id", "credential_type"),
    [
        (DEFAULT_CONNECTION_CLIENT_SECRET, ClientSecretCredential),
        (DEFAULT_CONNECTION_DEFAULT_CREDENTIAL, DefaultAzureCredential),
    ],
)
def test_get_connection_by_credential_client_secret(connection_id: str, credential_type: Type):
    hook = AzureSynapseHook(connection_id)

    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()
        connection = hook.get_conn()
        assert connection is not None
        mock_create_client.assert_called_once()
        assert isinstance(mock_create_client.call_args[0][0], credential_type)
        assert mock_create_client.call_args[0][-1] == "subscriptionId"


def test_run_spark_job(hook: AzureSynapseHook):
    hook.run_spark_job({})  # type: ignore

    if hook._conn is not None and isinstance(hook._conn, SparkClient):
        hook._conn.spark_batch.create_spark_batch_job.assert_called_with({})  # type: ignore[attr-defined]


def test_get_job_run_status(hook: AzureSynapseHook):
    hook.get_job_run_status()
    if hook._conn is not None and isinstance(hook._conn, SparkClient):
        hook._conn.spark_batch.get_spark_batch_job.assert_called_with(  # type: ignore[attr-defined]
            batch_id=JOB_ID
        )


_wait_for_job_run_status_test_args = [
    (AzureSynapseSparkBatchRunStatus.SUCCESS, AzureSynapseSparkBatchRunStatus.SUCCESS, True),
    (AzureSynapseSparkBatchRunStatus.ERROR, AzureSynapseSparkBatchRunStatus.SUCCESS, False),
    (AzureSynapseSparkBatchRunStatus.KILLED, AzureSynapseSparkBatchRunStatus.SUCCESS, False),
    (AzureSynapseSparkBatchRunStatus.RUNNING, AzureSynapseSparkBatchRunStatus.SUCCESS, "timeout"),
    (AzureSynapseSparkBatchRunStatus.NOT_STARTED, AzureSynapseSparkBatchRunStatus.SUCCESS, "timeout"),
    (AzureSynapseSparkBatchRunStatus.SHUTTING_DOWN, AzureSynapseSparkBatchRunStatus.SUCCESS, "timeout"),
    (AzureSynapseSparkBatchRunStatus.SUCCESS, AzureSynapseSparkBatchRunStatus.TERMINAL_STATUSES, True),
    (AzureSynapseSparkBatchRunStatus.ERROR, AzureSynapseSparkBatchRunStatus.TERMINAL_STATUSES, True),
    (AzureSynapseSparkBatchRunStatus.KILLED, AzureSynapseSparkBatchRunStatus.TERMINAL_STATUSES, True),
]


@pytest.mark.parametrize(
    argnames=("job_run_status", "expected_status", "expected_output"),
    argvalues=_wait_for_job_run_status_test_args,
    ids=[
        f"run_status_{argval[0]}_expected_{argval[1]}"
        if isinstance(argval[1], str)
        else f"run_status_{argval[0]}_expected_AnyTerminalStatus"
        for argval in _wait_for_job_run_status_test_args
    ],
)
def test_wait_for_job_run_status(hook, job_run_status, expected_status, expected_output):
    config = {"job_id": ID, "timeout": 3, "check_interval": 1, "expected_statuses": expected_status}

    with patch.object(AzureSynapseHook, "get_job_run_status") as mock_job_run:
        mock_job_run.return_value = job_run_status

        if expected_output != "timeout":
            assert hook.wait_for_job_run_status(**config) == expected_output
        else:
            with pytest.raises(Exception):
                hook.wait_for_job_run_status(**config)


def test_cancel_job_run(hook: AzureSynapseHook):
    hook.cancel_job_run(JOB_ID)
    if hook._conn is not None and isinstance(hook._conn, SparkClient):
        hook._conn.spark_batch.cancel_spark_batch_job.assert_called_with(JOB_ID)  # type: ignore[attr-defined]
