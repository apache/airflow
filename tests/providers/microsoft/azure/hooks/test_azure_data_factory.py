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

import json
import os
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory.aio import DataFactoryManagementClient
from azure.mgmt.datafactory.models import FactoryListResponse
from pytest import fixture, param

from airflow import AirflowException
from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryAsyncHook,
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
    get_field,
    provide_targeted_factory,
)
from airflow.utils import db

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


def setup_module():
    connection_client_secret = Connection(
        conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
        conn_type="azure_data_factory",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "tenantId": "tenantId",
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            }
        ),
    )
    connection_default_credential = Connection(
        conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
        conn_type="azure_data_factory",
        extra=json.dumps(
            {
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            }
        ),
    )
    connection_missing_subscription_id = Connection(
        conn_id="azure_data_factory_missing_subscription_id",
        conn_type="azure_data_factory",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "tenantId": "tenantId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            }
        ),
    )
    connection_missing_tenant_id = Connection(
        conn_id="azure_data_factory_missing_tenant_id",
        conn_type="azure_data_factory",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "subscriptionId": "subscriptionId",
                "resource_group_name": DEFAULT_RESOURCE_GROUP,
                "factory_name": DEFAULT_FACTORY,
            }
        ),
    )

    db.merge_conn(connection_client_secret)
    db.merge_conn(connection_default_credential)
    db.merge_conn(connection_missing_subscription_id)
    db.merge_conn(connection_missing_tenant_id)


@fixture
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

    with pytest.raises(AirflowException):
        conn.extra_dejson = {}
        provide_targeted_factory(echo)(hook)


@pytest.mark.parametrize(
    ("connection_id", "credential_type"),
    [
        (DEFAULT_CONNECTION_CLIENT_SECRET, ClientSecretCredential),
        (DEFAULT_CONNECTION_DEFAULT_CREDENTIAL, DefaultAzureCredential),
    ],
)
def test_get_connection_by_credential_client_secret(connection_id: str, credential_type: type):
    hook = AzureDataFactoryHook(connection_id)

    with patch.object(hook, "_create_client") as mock_create_client:
        mock_create_client.return_value = MagicMock()
        connection = hook.get_conn()
        assert connection is not None
        mock_create_client.assert_called_once()
        assert isinstance(mock_create_client.call_args[0][0], credential_type)
        assert mock_create_client.call_args[0][1] == "subscriptionId"


@parametrize(
    explicit_factory=((RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY)),
    implicit_factory=((), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)),
)
def test_get_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_factory(*user_args)

    hook._conn.factories.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_create_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_factory(*user_args)

    hook._conn.factories.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_update_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_factory_exists") as mock_factory_exists:
        mock_factory_exists.return_value = True
        hook.update_factory(*user_args)

    hook._conn.factories.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_update_factory_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_factory_exists") as mock_factory_exists:
        mock_factory_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Factory .+ does not exist"):
        hook.update_factory(*user_args)


@parametrize(
    explicit_factory=((RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY)),
    implicit_factory=((), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)),
)
def test_delete_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_factory(*user_args)

    hook._conn.factories.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_linked_service(*user_args)

    hook._conn.linked_services.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_linked_service(*user_args)

    hook._conn.linked_services.create_or_update(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_linked_service_exists") as mock_linked_service_exists:
        mock_linked_service_exists.return_value = True
        hook.update_linked_service(*user_args)

    hook._conn.linked_services.create_or_update(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_linked_service_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_linked_service_exists") as mock_linked_service_exists:
        mock_linked_service_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Linked service .+ does not exist"):
        hook.update_linked_service(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_linked_service(*user_args)

    hook._conn.linked_services.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_dataset(*user_args)

    hook._conn.datasets.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_dataset(*user_args)

    hook._conn.datasets.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_dataset_exists") as mock_dataset_exists:
        mock_dataset_exists.return_value = True
        hook.update_dataset(*user_args)

    hook._conn.datasets.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataset_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_dataset_exists") as mock_dataset_exists:
        mock_dataset_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Dataset .+ does not exist"):
        hook.update_dataset(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_dataset(*user_args)

    hook._conn.datasets.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_dataflow(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_dataflow(*user_args)

    hook._conn.data_flows.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_dataflow(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_dataflow(*user_args)

    hook._conn.data_flows.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataflow(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_dataflow_exists") as mock_dataflow_exists:
        mock_dataflow_exists.return_value = True
        hook.update_dataflow(*user_args)

    hook._conn.data_flows.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataflow_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_dataflow_exists") as mock_dataflow_exists:
        mock_dataflow_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Dataflow .+ does not exist"):
        hook.update_dataflow(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=(
        (NAME,),
        (
            DEFAULT_RESOURCE_GROUP,
            DEFAULT_FACTORY,
            NAME,
        ),
    ),
)
def test_delete_dataflow(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_dataflow(*user_args)

    hook._conn.data_flows.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_pipeline(*user_args)

    hook._conn.pipelines.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_pipeline(*user_args)

    hook._conn.pipelines.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_pipeline_exists") as mock_pipeline_exists:
        mock_pipeline_exists.return_value = True
        hook.update_pipeline(*user_args)

    hook._conn.pipelines.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_pipeline_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_pipeline_exists") as mock_pipeline_exists:
        mock_pipeline_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Pipeline .+ does not exist"):
        hook.update_pipeline(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_pipeline(*user_args)

    hook._conn.pipelines.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_run_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.run_pipeline(*user_args)

    hook._conn.pipelines.create_run.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, ID)),
    implicit_factory=((ID,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, ID)),
)
def test_get_pipeline_run(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_pipeline_run(*user_args)

    hook._conn.pipeline_runs.get.assert_called_with(*sdk_args)


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
    config = {"run_id": ID, "timeout": 3, "check_interval": 1, "expected_statuses": expected_status}

    with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_pipeline_run:
        mock_pipeline_run.return_value.status = pipeline_run_status

        if expected_output != "timeout":
            assert hook.wait_for_pipeline_run_status(**config) == expected_output
        else:
            with pytest.raises(AzureDataFactoryPipelineRunException):
                hook.wait_for_pipeline_run_status(**config)


@parametrize(
    explicit_factory=((ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, ID)),
    implicit_factory=((ID,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, ID)),
)
def test_cancel_pipeline_run(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.cancel_pipeline_run(*user_args)

    hook._conn.pipeline_runs.cancel.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_trigger(*user_args)

    hook._conn.triggers.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_trigger(*user_args)

    hook._conn.triggers.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_trigger_exists") as mock_trigger_exists:
        mock_trigger_exists.return_value = True
        hook.update_trigger(*user_args)

    hook._conn.triggers.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_trigger_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    with patch.object(hook, "_trigger_exists") as mock_trigger_exists:
        mock_trigger_exists.return_value = False

    with pytest.raises(AirflowException, match=r"Trigger .+ does not exist"):
        hook.update_trigger(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_trigger(*user_args)

    hook._conn.triggers.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_start_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.start_trigger(*user_args)

    hook._conn.triggers.begin_start.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_stop_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.stop_trigger(*user_args)

    hook._conn.triggers.begin_stop.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, ID)),
    implicit_factory=((NAME, ID), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, ID)),
)
def test_rerun_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.rerun_trigger(*user_args)

    hook._conn.trigger_runs.rerun.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, ID)),
    implicit_factory=((NAME, ID), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, ID)),
)
def test_cancel_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.cancel_trigger(*user_args)

    hook._conn.trigger_runs.cancel.assert_called_with(*sdk_args)


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
        param(
            "a://?extra__azure_data_factory__resource_group_name=abc"
            "&extra__azure_data_factory__factory_name=abc",
            id="prefix",
        ),
        param("a://?resource_group_name=abc&factory_name=abc", id="no-prefix"),
    ],
)
@patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_conn")
def test_provide_targeted_factory_backcompat_prefix_works(mock_connect, uri):
    with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
        hook = AzureDataFactoryHook("my_conn")
        hook.delete_factory()
        mock_connect.return_value.factories.delete.assert_called_with("abc", "abc")


@pytest.mark.parametrize(
    "uri",
    [
        param(
            "a://hi:yo@?extra__azure_data_factory__tenantId=ten"
            "&extra__azure_data_factory__subscriptionId=sub",
            id="prefix",
        ),
        param("a://hi:yo@?tenantId=ten&subscriptionId=sub", id="no-prefix"),
    ],
)
@patch("airflow.providers.microsoft.azure.hooks.data_factory.ClientSecretCredential")
@patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook._create_client")
def test_get_conn_backcompat_prefix_works(mock_create, mock_cred, uri):
    with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
        hook = AzureDataFactoryHook("my_conn")
        hook.get_conn()
        mock_cred.assert_called_with(client_id="hi", client_secret="yo", tenant_id="ten")
        mock_create.assert_called_with(mock_cred.return_value, "sub")


@patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_conn")
def test_backcompat_prefix_both_prefers_short(mock_connect):
    with patch.dict(
        os.environ,
        {
            "AIRFLOW_CONN_MY_CONN": "a://?resource_group_name=non-prefixed"
            "&extra__azure_data_factory__resource_group_name=prefixed"
        },
    ):
        hook = AzureDataFactoryHook("my_conn")
        hook.delete_factory(factory_name="n/a")
        mock_connect.return_value.factories.delete.assert_called_with("non-prefixed", "n/a")


class TestAzureDataFactoryAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_queued(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Queued"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_success(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Succeeded"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_failed(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Failed"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_cancelled(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_status = "Cancelled"
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_exception(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with exception"""
        mock_get_pipeline_run.side_effect = Exception("Test exception")
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_connection")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    async def test_get_pipeline_run_exception_without_resource(
        self, mock_conn, mock_get_connection, mock_pipeline_run
    ):
        """
        Test get_pipeline_run function without passing the resource name to check the decorator function and
        raise exception
        """
        mock_connection = Connection(
            extra=json.dumps({"extra__azure_data_factory__factory_name": DATAFACTORY_NAME})
        )
        mock_get_connection.return_value = mock_connection
        mock_conn.return_value.pipeline_runs.get.return_value = mock_pipeline_run
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_pipeline_run(RUN_ID, None, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_async_conn")
    async def test_get_pipeline_run_exception(self, mock_conn):
        """Test get_pipeline_run function with exception"""
        mock_conn.return_value.pipeline_runs.get.side_effect = Exception("Test exception")
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_pipeline_run(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_connection")
    async def test_get_async_conn(self, mock_connection):
        """"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_connection")
    async def test_get_async_conn_without_login_id(self, mock_connection):
        """Test get_async_conn function without login id"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_connection_params",
        [
            {
                "extra__azure_data_factory__tenantId": "tenantId",
                "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
            }
        ],
    )
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_connection")
    async def test_get_async_conn_key_error_subscription_id(self, mock_connection, mock_connection_params):
        """Test get_async_conn function when subscription_id is missing in the connection"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra=json.dumps(mock_connection_params),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(ValueError):
            await hook.get_async_conn()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_connection_params",
        [
            {
                "extra__azure_data_factory__subscriptionId": "subscriptionId",
                "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
            },
        ],
    )
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_connection")
    async def test_get_async_conn_key_error_tenant_id(self, mock_connection, mock_connection_params):
        """Test get_async_conn function when tenant id is missing in the connection"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra=json.dumps(mock_connection_params),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryAsyncHook(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(ValueError):
            await hook.get_async_conn()

    def test_get_field_prefixed_extras(self):
        """Test get_field function for retrieving prefixed extra fields"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
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
            extra=json.dumps(
                {
                    "tenantId": "tenantId",
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        extras = mock_conn.extra_dejson
        assert get_field(extras, "tenantId", strict=True) == "tenantId"
        assert get_field(extras, "subscriptionId", strict=True) == "subscriptionId"
        assert get_field(extras, "resource_group_name", strict=True) == RESOURCE_GROUP_NAME
        assert get_field(extras, "factory_name", strict=True) == DATAFACTORY_NAME
        with pytest.raises(KeyError):
            get_field(extras, "non-existent-field", strict=True)
