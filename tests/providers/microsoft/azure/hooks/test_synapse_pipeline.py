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

from unittest.mock import MagicMock, patch

import pytest
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient

from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.synapse import (
    AzureSynapsePipelineHook,
    AzureSynapsePipelineRunException,
    AzureSynapsePipelineRunStatus,
)

DEFAULT_CONNECTION_CLIENT_SECRET = "azure_synapse_test_client_secret"
DEFAULT_CONNECTION_DEFAULT_CREDENTIAL = "azure_synapse_test_default_credential"

SYNAPSE_WORKSPACE_URL = "synapse_workspace_url"
AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT = "azure_synapse_workspace_dev_endpoint"
PIPELINE_NAME = "pipeline_name"
RUN_ID = "run_id"


@pytest.fixture(autouse=True)
def setup_connections(create_mock_connections):
    create_mock_connections(
        # connection_client_secret
        Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_synapse",
            host=SYNAPSE_WORKSPACE_URL,
            login="clientId",
            password="clientSecret",
            extra={"tenantId": "tenantId"},
        ),
        # connection_default_credential
        Connection(
            conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
            conn_type="azure_synapse",
            host=SYNAPSE_WORKSPACE_URL,
            extra={},
        ),
        # connection_missing_tenant_id
        Connection(
            conn_id="azure_synapse_missing_tenant_id",
            conn_type="azure_synapse",
            host=SYNAPSE_WORKSPACE_URL,
            login="clientId",
            password="clientSecret",
            extra={},
        ),
    )


@pytest.fixture
def hook():
    client = AzureSynapsePipelineHook(
        azure_synapse_conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
        azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
    )
    client._conn = MagicMock(spec=["pipeline_run", "pipeline"])

    return client


@pytest.mark.parametrize(
    ("connection_id", "credential_type"),
    [
        (DEFAULT_CONNECTION_CLIENT_SECRET, ClientSecretCredential),
        (DEFAULT_CONNECTION_DEFAULT_CREDENTIAL, DefaultAzureCredential),
    ],
)
def test_get_connection_by_credential_client_secret(connection_id: str, credential_type: type):
    hook = AzureSynapsePipelineHook(
        azure_synapse_conn_id=connection_id,
        azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
    )

    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()
        connection = hook.get_conn()
        assert connection is not None
        mock_create_client.assert_called_once()
        assert isinstance(mock_create_client.call_args.args[0], credential_type)
        assert mock_create_client.call_args.args[1] == AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT


def test_run_pipeline(hook: AzureSynapsePipelineHook):
    hook.run_pipeline(PIPELINE_NAME)

    if hook._conn is not None and isinstance(hook._conn, ArtifactsClient):
        hook._conn.pipeline.create_pipeline_run.assert_called_with(PIPELINE_NAME)


def test_get_pipeline_run(hook: AzureSynapsePipelineHook):
    hook.get_pipeline_run(run_id=RUN_ID)

    if hook._conn is not None and isinstance(hook._conn, ArtifactsClient):
        hook._conn.pipeline_run.get_pipeline_run.assert_called_with(run_id=RUN_ID)


def test_cancel_run_pipeline(hook: AzureSynapsePipelineHook):
    hook.cancel_run_pipeline(RUN_ID)

    if hook._conn is not None and isinstance(hook._conn, ArtifactsClient):
        hook._conn.pipeline_run.cancel_pipeline_run.assert_called_with(RUN_ID)


_wait_for_pipeline_run_status_test_args = [
    (AzureSynapsePipelineRunStatus.SUCCEEDED, AzureSynapsePipelineRunStatus.SUCCEEDED, True),
    (AzureSynapsePipelineRunStatus.FAILED, AzureSynapsePipelineRunStatus.SUCCEEDED, False),
    (AzureSynapsePipelineRunStatus.CANCELLED, AzureSynapsePipelineRunStatus.SUCCEEDED, False),
    (AzureSynapsePipelineRunStatus.IN_PROGRESS, AzureSynapsePipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureSynapsePipelineRunStatus.QUEUED, AzureSynapsePipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureSynapsePipelineRunStatus.CANCELING, AzureSynapsePipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureSynapsePipelineRunStatus.SUCCEEDED, AzureSynapsePipelineRunStatus.TERMINAL_STATUSES, True),
    (AzureSynapsePipelineRunStatus.FAILED, AzureSynapsePipelineRunStatus.TERMINAL_STATUSES, True),
    (AzureSynapsePipelineRunStatus.CANCELLED, AzureSynapsePipelineRunStatus.TERMINAL_STATUSES, True),
]


@pytest.mark.parametrize(
    argnames=("pipeline_run_status", "expected_status", "expected_output"),
    argvalues=_wait_for_pipeline_run_status_test_args,
    ids=[
        f"run_status_{argval[0]}_expected_{argval[1]}"
        if isinstance(argval[1], str)
        else f"run_status_{argval[0]}_expected_AnyTerminalStatus"
        for argval in _wait_for_pipeline_run_status_test_args
    ],
)
def test_wait_for_pipeline_run_status(hook, pipeline_run_status, expected_status, expected_output):
    config = {"run_id": RUN_ID, "timeout": 3, "check_interval": 1, "expected_statuses": expected_status}

    with patch.object(AzureSynapsePipelineHook, "get_pipeline_run") as mock_pipeline_run:
        mock_pipeline_run.return_value.status = pipeline_run_status

        if expected_output != "timeout":
            assert hook.wait_for_pipeline_run_status(**config) == expected_output
        else:
            with pytest.raises(AzureSynapsePipelineRunException):
                hook.wait_for_pipeline_run_status(**config)
