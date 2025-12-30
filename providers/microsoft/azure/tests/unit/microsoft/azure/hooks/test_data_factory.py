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
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from azure.mgmt.datafactory.aio import DataFactoryManagementClient
from azure.mgmt.datafactory.models import FactoryListResponse

from airflow.models.connection import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryAsyncHook,
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
    get_field,
    provide_targeted_factory,
)

DEFAULT_RESOURCE_GROUP = "defaultResourceGroup"
AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_default"
RESOURCE_GROUP_NAME = "team_provider_resource_group_test"
DATAFACTORY_NAME = "ADFProvidersTeamDataFactory"
TASK_ID = "test_adf_pipeline_run_status_sensor"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
DEFAULT_CONNECTION_CLIENT_SECRET = "azure_data_factory_test_client_secret"
MODULE = "airflow.providers.microsoft.azure"

RESOURCE_GROUP = "testResourceGroup"
DEFAULT_FACTORY = "defaultFactory"
FACTORY = "testFactory"

DEFAULT_CONNECTION_DEFAULT_CREDENTIAL = "azure_data_factory_test_default_credential"

MODEL = object()
NAME = "testName"
ID = "testId"

MODULE = "airflow.providers.microsoft.azure.hooks.data_factory"

# TODO: FIXME: the tests here have tricky issues with typing and need a bit more thought to fix them
# mypy: disable-error-code="union-attr,call-overload"

pytestmark = pytest.mark.db_test

if os.environ.get("_AIRFLOW_SKIP_DB_TESTS") == "true":
    # Handle collection of the test by non-db case
    Connection = mock.MagicMock()  # type: ignore[misc]


@pytest.fixture(autouse=True)
def setup_connections(create_mock_connections):
    create_mock_connections(
        # connection_client_secret
        Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra={
                "tenantId": "tenantId",
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            },
        ),
        # connection_default_credential
        Connection(
            conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
            conn_type="azure_data_factory",
            extra={
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            },
        ),
        # connection_missing_subscription_id
        Connection(
            conn_id="azure_data_factory_missing_subscription_id",
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra={
                "tenantId": "tenantId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            },
        ),
        # connection_missing_tenant_id
        Connection(
            conn_id="azure_data_factory_missing_tenant_id",
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra={
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            },
        ),
        # connection_workload_identity
        Connection(
            conn_id="azure_data_factory_workload_identity",
            conn_type="azure_data_factory",
            extra={
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
                "workload_identity_tenant_id": "workload_tenant_id",
                "managed_identity_client_id": "workload_client_id",
            },
        ),
    )


@pytest.fixture
def hook():
    client = AzureDataFactoryHook(azure_data_factory_conn_id=DEFAULT_CONNECTION_CLIENT_SECRET)
    client._conn = MagicMock(
        spec=[
            "factories",
            "linked_services",
            "datasets",
            "pipelines",
            "pipeline_runs",
            "triggers",
            "trigger_runs",
            "data_flows",
        ]
    )

    return client


def parametrize(explicit_factory, implicit_factory):
    def wrapper(func):
        return pytest.mark.parametrize(
            ("user_args", "sdk_args"),
            (explicit_factory, implicit_factory),
            ids=("explicit factory", "implicit factory"),
        )(func)

    return wrapper


def test_provide_targeted_factory():
    def echo(_, resource_group_name=None, factory_name=None):
        return resource_group_name, factory_name

    conn = MagicMock()
    hook = MagicMock()
    hook.get_connection.return_value = conn

    conn.extra_dejson = {}
    assert provide_targeted_factory(echo)(hook, RESOURCE_GROUP, FACTORY) == (RESOURCE_GROUP, FACTORY)

    conn.extra_dejson = {
        "resource_group_name": DEFAULT_RESOURCE_GROUP,
        "factory_name": DEFAULT_FACTORY,
    }
    assert provide_targeted_factory(echo)(hook) == (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)
    assert provide_targeted_factory(echo)(hook, RESOURCE_GROUP, None) == (RESOURCE_GROUP, DEFAULT_FACTORY)
    assert provide_targeted_factory(echo)(hook, None, FACTORY) == (DEFAULT_RESOURCE_GROUP, FACTORY)
    assert provide_targeted_factory(echo)(hook, None, None) == (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)

    conn.extra_dejson = {}
    with pytest.raises(AirflowException, match="Could not determine the targeted data factory"):
        provide_targeted_factory(echo)(hook)


@mock.patch(f"{MODULE}.ClientSecretCredential")
def test_get_conn_by_credential_client_secret(mock_credential):
    hook = AzureDataFactoryHook(DEFAULT_CONNECTION_CLIENT_SECRET)

    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()

        connection = hook.get_conn()
        assert connection is not None

        mock_create_client.assert_called_with(mock_credential(), "subscriptionId")


@mock.patch(f"{MODULE}.get_sync_default_azure_credential")
def test_get_conn_by_default_azure_credential(mock_credential):
    hook = AzureDataFactoryHook(DEFAULT_CONNECTION_DEFAULT_CREDENTIAL)

    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()

        connection = hook.get_conn()
        assert connection is not None
        mock_credential.assert_called()
        args = mock_credential.call_args
        assert args.kwargs["managed_identity_client_id"] is None
        assert args.kwargs["workload_identity_tenant_id"] is None

        mock_create_client.assert_called_with(mock_credential(), "subscriptionId")


@mock.patch(f"{MODULE}.get_sync_default_azure_credential")
def test_get_conn_with_workload_identity(mock_credential):
    hook = AzureDataFactoryHook("azure_data_factory_workload_identity")
    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()

        connection = hook.get_conn()
        assert connection is not None
        mock_credential.assert_called_once_with(
            managed_identity_client_id="workload_client_id",
            workload_identity_tenant_id="workload_tenant_id",
        )
        mock_create_client.assert_called_with(mock_credential(), "subscriptionId")


def test_get_factory(hook: AzureDataFactoryHook):
    hook.get_factory(RESOURCE_GROUP, FACTORY)

    hook._conn.factories.get.assert_called_with(RESOURCE_GROUP, FACTORY)


def test_create_factory(hook: AzureDataFactoryHook):
    hook.create_factory(MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.factories.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, MODEL)


def test_update_factory(hook: AzureDataFactoryHook):
    with patch.object(hook, "_factory_exists") as mock_factory_exists:
        mock_factory_exists.return_value = True
        hook.update_factory(MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.factories.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, MODEL, None)


def test_update_factory_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_factory_exists") as mock_factory_exists:
        mock_factory_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Factory .+ does not exist"):
        hook.update_factory(MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_factory(hook: AzureDataFactoryHook):
    hook.delete_factory(RESOURCE_GROUP, FACTORY)

    hook._conn.factories.delete.assert_called_with(RESOURCE_GROUP, FACTORY)


def test_get_linked_service(hook: AzureDataFactoryHook):
    hook.get_linked_service(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.linked_services.get.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, None)


def test_create_linked_service(hook: AzureDataFactoryHook):
    hook.create_linked_service(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.linked_services.create_or_update(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_linked_service(hook: AzureDataFactoryHook):
    with patch.object(hook, "_linked_service_exists") as mock_linked_service_exists:
        mock_linked_service_exists.return_value = True
        hook.update_linked_service(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.linked_services.create_or_update(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_linked_service_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_linked_service_exists") as mock_linked_service_exists:
        mock_linked_service_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Linked service .+ does not exist"):
        hook.update_linked_service(NAME, MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_linked_service(hook: AzureDataFactoryHook):
    hook.delete_linked_service(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.linked_services.delete.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_get_dataset(hook: AzureDataFactoryHook):
    hook.get_dataset(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.datasets.get.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_create_dataset(hook: AzureDataFactoryHook):
    hook.create_dataset(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.datasets.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_dataset(hook: AzureDataFactoryHook):
    with patch.object(hook, "_dataset_exists") as mock_dataset_exists:
        mock_dataset_exists.return_value = True
        hook.update_dataset(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.datasets.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_dataset_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_dataset_exists") as mock_dataset_exists:
        mock_dataset_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Dataset .+ does not exist"):
        hook.update_dataset(NAME, MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_dataset(hook: AzureDataFactoryHook):
    hook.delete_dataset(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.datasets.delete.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_get_dataflow(hook: AzureDataFactoryHook):
    hook.get_dataflow(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.data_flows.get.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, None)


def test_create_dataflow(hook: AzureDataFactoryHook):
    hook.create_dataflow(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.data_flows.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL, None)


def test_update_dataflow(hook: AzureDataFactoryHook):
    with patch.object(hook, "_dataflow_exists") as mock_dataflow_exists:
        mock_dataflow_exists.return_value = True
        hook.update_dataflow(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.data_flows.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL, None)


def test_update_dataflow_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_dataflow_exists") as mock_dataflow_exists:
        mock_dataflow_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Dataflow .+ does not exist"):
        hook.update_dataflow(NAME, MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_dataflow(hook: AzureDataFactoryHook):
    hook.delete_dataflow(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.data_flows.delete.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_get_pipeline(hook: AzureDataFactoryHook):
    hook.get_pipeline(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.pipelines.get.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_create_pipeline(hook: AzureDataFactoryHook):
    hook.create_pipeline(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.pipelines.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_pipeline(hook: AzureDataFactoryHook):
    with patch.object(hook, "_pipeline_exists") as mock_pipeline_exists:
        mock_pipeline_exists.return_value = True
        hook.update_pipeline(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.pipelines.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_pipeline_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_pipeline_exists") as mock_pipeline_exists:
        mock_pipeline_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Pipeline .+ does not exist"):
        hook.update_pipeline(NAME, MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_pipeline(hook: AzureDataFactoryHook):
    hook.delete_pipeline(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.pipelines.delete.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_run_pipeline(hook: AzureDataFactoryHook):
    hook.run_pipeline(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.pipelines.create_run.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_get_pipeline_run(hook: AzureDataFactoryHook):
    hook.get_pipeline_run(ID, RESOURCE_GROUP, FACTORY)

    hook._conn.pipeline_runs.get.assert_called_with(RESOURCE_GROUP, FACTORY, ID)


_wait_for_pipeline_run_status_test_args = [
    (AzureDataFactoryPipelineRunStatus.SUCCEEDED, AzureDataFactoryPipelineRunStatus.SUCCEEDED, True),
    (AzureDataFactoryPipelineRunStatus.FAILED, AzureDataFactoryPipelineRunStatus.SUCCEEDED, False),
    (AzureDataFactoryPipelineRunStatus.CANCELLED, AzureDataFactoryPipelineRunStatus.SUCCEEDED, False),
    (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, AzureDataFactoryPipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureDataFactoryPipelineRunStatus.QUEUED, AzureDataFactoryPipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureDataFactoryPipelineRunStatus.CANCELING, AzureDataFactoryPipelineRunStatus.SUCCEEDED, "timeout"),
    (AzureDataFactoryPipelineRunStatus.SUCCEEDED, AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES, True),
    (AzureDataFactoryPipelineRunStatus.FAILED, AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES, True),
    (AzureDataFactoryPipelineRunStatus.CANCELLED, AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES, True),
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
    config = {
        "resource_group_name": RESOURCE_GROUP,
        "factory_name": FACTORY,
        "run_id": ID,
        "timeout": 3,
        "check_interval": 1,
        "expected_statuses": expected_status,
    }

    with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_pipeline_run:
        mock_pipeline_run.return_value.status = pipeline_run_status

        if expected_output != "timeout":
            assert hook.wait_for_pipeline_run_status(**config) == expected_output
        else:
            with pytest.raises(AzureDataFactoryPipelineRunException):
                hook.wait_for_pipeline_run_status(**config)


def test_cancel_pipeline_run(hook: AzureDataFactoryHook):
    hook.cancel_pipeline_run(ID, RESOURCE_GROUP, FACTORY)

    hook._conn.pipeline_runs.cancel.assert_called_with(RESOURCE_GROUP, FACTORY, ID)


def test_get_trigger(hook: AzureDataFactoryHook):
    hook.get_trigger(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.get.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_create_trigger(hook: AzureDataFactoryHook):
    hook.create_trigger(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL)


def test_update_trigger(hook: AzureDataFactoryHook):
    with patch.object(hook, "_trigger_exists") as mock_trigger_exists:
        mock_trigger_exists.return_value = True
        hook.update_trigger(NAME, MODEL, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.create_or_update.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, MODEL, None)


def test_update_trigger_non_existent(hook: AzureDataFactoryHook):
    with patch.object(hook, "_trigger_exists") as mock_trigger_exists:
        mock_trigger_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Trigger .+ does not exist"):
        hook.update_trigger(NAME, MODEL, RESOURCE_GROUP, FACTORY)


def test_delete_trigger(hook: AzureDataFactoryHook):
    hook.delete_trigger(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.delete.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_start_trigger(hook: AzureDataFactoryHook):
    hook.start_trigger(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.begin_start.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_stop_trigger(hook: AzureDataFactoryHook):
    hook.stop_trigger(NAME, RESOURCE_GROUP, FACTORY)

    hook._conn.triggers.begin_stop.assert_called_with(RESOURCE_GROUP, FACTORY, NAME)


def test_rerun_trigger(hook: AzureDataFactoryHook):
    hook.rerun_trigger(NAME, ID, RESOURCE_GROUP, FACTORY)

    hook._conn.trigger_runs.rerun.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, ID)


def test_cancel_trigger(hook: AzureDataFactoryHook):
    hook.cancel_trigger(NAME, ID, RESOURCE_GROUP, FACTORY)

    hook._conn.trigger_runs.cancel.assert_called_with(RESOURCE_GROUP, FACTORY, NAME, ID)


@pytest.mark.parametrize(
    argnames="factory_list_result",
    argvalues=[iter([FactoryListResponse]), iter([])],
    ids=["factory_exists", "factory_does_not_exist"],
)
def test_connection_success(hook, factory_list_result):
    hook.get_conn().factories.list.return_value = factory_list_result
    status, msg = hook.test_connection()

    assert status is True
    assert msg == "Successfully connected to Azure Data Factory."


def test_connection_failure(hook):
    hook.get_conn().factories.list = PropertyMock(side_effect=Exception("Authentication failed."))
    status, msg = hook.test_connection()

    assert status is False
    assert msg == "Authentication failed."


def test_connection_failure_missing_subscription_id():
    hook = AzureDataFactoryHook("azure_data_factory_missing_subscription_id")
    status, msg = hook.test_connection()

    assert status is False
    assert msg == "A Subscription ID is required to connect to Azure Data Factory."


def test_connection_failure_missing_tenant_id():
    hook = AzureDataFactoryHook("azure_data_factory_missing_tenant_id")
    status, msg = hook.test_connection()

    assert status is False
    assert msg == "A Tenant ID is required when authenticating with Client ID and Secret."


@pytest.mark.parametrize(
    "uri",
    [
        pytest.param(
            "a://?extra__azure_data_factory__resource_group_name=abc"
            "&extra__azure_data_factory__factory_name=abc",
            id="prefix",
        ),
        pytest.param("a://?resource_group_name=abc&factory_name=abc", id="no-prefix"),
    ],
)
@patch(f"{MODULE}.AzureDataFactoryHook.get_conn")
def test_provide_targeted_factory_backcompat_prefix_works(mock_connect, uri):
    with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
        hook = AzureDataFactoryHook("my_conn")
        hook.delete_factory(RESOURCE_GROUP, FACTORY)
        mock_connect.return_value.factories.delete.assert_called_with(RESOURCE_GROUP, FACTORY)


@pytest.mark.parametrize(
    "uri",
    [
        pytest.param(
            "a://hi:yo@?extra__azure_data_factory__tenantId=ten"
            "&extra__azure_data_factory__subscriptionId=sub",
            id="prefix",
        ),
        pytest.param("a://hi:yo@?tenantId=ten&subscriptionId=sub", id="no-prefix"),
    ],
)
@patch(f"{MODULE}.ClientSecretCredential")
@patch(f"{MODULE}.AzureDataFactoryHook._create_client")
def test_get_conn_backcompat_prefix_works(mock_create, mock_cred, uri):
    with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
        hook = AzureDataFactoryHook("my_conn")
        hook.get_conn()
        mock_cred.assert_called_with(client_id="hi", client_secret="yo", tenant_id="ten")
        mock_create.assert_called_with(mock_cred.return_value, "sub")


@patch(f"{MODULE}.AzureDataFactoryHook.get_conn")
def test_backcompat_prefix_both_prefers_short(mock_connect):
    with patch.dict(
        os.environ,
        {
            "AIRFLOW_CONN_MY_CONN": "a://?resource_group_name=non-prefixed"
            "&extra__azure_data_factory__resource_group_name=prefixed"
        },
    ):
        hook = AzureDataFactoryHook("my_conn")
        hook.delete_factory(RESOURCE_GROUP, FACTORY)
        mock_connect.return_value.factories.delete.assert_called_with(RESOURCE_GROUP, FACTORY)


def test_refresh_conn(hook):
    """Test refresh_conn method _conn is reset and get_conn is called"""
    with patch.object(hook, "get_conn") as mock_get_conn:
        hook.refresh_conn()
        assert not hook._conn
        assert mock_get_conn.called


class TestAzureDataFactoryAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_queued(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Queued"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_inprogress(
        self,
        mock_get_pipeline_run,
        mock_conn,
    ):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "InProgress"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_success(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Succeeded"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_failed(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Failed"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_cancelled(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Cancelled"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_connection")
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    async def test_get_pipeline_run_exception_without_resource(
        self, mock_conn, mock_get_connection, mock_pipeline_run
    ):
        """
        Test get_pipeline_run function without passing the resource name to check the decorator function and
        raise exception
        """
        mock_connection = Connection(extra={"factory_name": DATAFACTORY_NAME})
        mock_get_connection.return_value = mock_connection
        mock_conn.return_value.pipeline_runs.get.return_value = mock_pipeline_run
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_pipeline_run(RUN_ID, None, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
                conn_type="azure_data_factory",
                extra={
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                    "managed_identity_client_id": "test_client_id",
                    "workload_identity_tenant_id": "test_tenant_id",
                },
            )
        ],
        indirect=True,
    )
    @mock.patch(f"{MODULE}.get_async_default_azure_credential")
    async def test_get_async_conn_with_default_azure_credential(
        self, mock_default_azure_credential, mocked_connection
    ):
        """"""
        hook = AzureDataFactoryAsyncHook(mocked_connection.conn_id)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

        mock_default_azure_credential.assert_called_with(
            managed_identity_client_id="test_client_id", workload_identity_tenant_id="test_tenant_id"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
                conn_type="azure_data_factory",
                login="clientId",
                password="clientSecret",
                extra={
                    "tenantId": "tenantId",
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                },
            )
        ],
        indirect=True,
    )
    async def test_get_async_conn(self, mocked_connection):
        """"""
        hook = AzureDataFactoryAsyncHook(mocked_connection.conn_id)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
                conn_type="azure_data_factory",
                extra={
                    "tenantId": "tenantId",
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                },
            ),
        ],
        indirect=True,
    )
    async def test_get_async_conn_without_login_id(self, mocked_connection):
        """Test get_async_conn function without login id"""
        hook = AzureDataFactoryAsyncHook(mocked_connection.conn_id)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
                conn_type="azure_data_factory",
                login="clientId",
                password="clientSecret",
                extra={
                    "tenantId": "tenantId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                },
            )
        ],
        indirect=True,
    )
    async def test_get_async_conn_key_error_subscription_id(self, mocked_connection):
        """Test get_async_conn function when subscription_id is missing in the connection"""
        hook = AzureDataFactoryAsyncHook(mocked_connection.conn_id)
        with pytest.raises(
            ValueError, match="A Subscription ID is required to connect to Azure Data Factory."
        ):
            await hook.get_async_conn()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
                conn_type="azure_data_factory",
                login="clientId",
                password="clientSecret",
                extra={
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                },
            )
        ],
        indirect=True,
    )
    async def test_get_async_conn_key_error_tenant_id(self, mocked_connection):
        """Test get_async_conn function when tenant id is missing in the connection"""
        hook = AzureDataFactoryAsyncHook(mocked_connection.conn_id)
        with pytest.raises(
            ValueError, match="A Tenant ID is required when authenticating with Client ID and Secret."
        ):
            await hook.get_async_conn()

    def test_get_field_prefixed_extras(self):
        """Test get_field function for retrieving prefixed extra fields"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra={
                "tenantId": "tenantId",
                "subscriptionId": "subscriptionId",
                "resource_group_name": RESOURCE_GROUP_NAME,
                "factory_name": DATAFACTORY_NAME,
            },
        )
        extras = mock_conn.extra_dejson
        assert get_field(extras, "tenantId", strict=True) == "tenantId"
        assert get_field(extras, "subscriptionId", strict=True) == "subscriptionId"
        assert get_field(extras, "resource_group_name", strict=True) == RESOURCE_GROUP_NAME
        assert get_field(extras, "factory_name", strict=True) == DATAFACTORY_NAME
        with pytest.raises(KeyError):
            get_field(extras, "non-existent-field", strict=True)

    def test_get_field_non_prefixed_extras(self):
        """Test get_field function for retrieving non-prefixed extra fields"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra={
                "tenantId": "tenantId",
                "subscriptionId": "subscriptionId",
                "resource_group_name": RESOURCE_GROUP_NAME,
                "factory_name": DATAFACTORY_NAME,
            },
        )
        extras = mock_conn.extra_dejson
        assert get_field(extras, "tenantId", strict=True) == "tenantId"
        assert get_field(extras, "subscriptionId", strict=True) == "subscriptionId"
        assert get_field(extras, "resource_group_name", strict=True) == RESOURCE_GROUP_NAME
        assert get_field(extras, "factory_name", strict=True) == DATAFACTORY_NAME
        with pytest.raises(KeyError):
            get_field(extras, "non-existent-field", strict=True)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureDataFactoryAsyncHook.get_async_conn")
    async def test_refresh_conn(self, mock_get_async_conn):
        """Test refresh_conn method _conn is reset and get_async_conn is called"""
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        await hook.refresh_conn()
        assert not hook._conn
        assert mock_get_async_conn.called
