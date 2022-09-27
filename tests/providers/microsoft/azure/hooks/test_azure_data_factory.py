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
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory.models import FactoryListResponse
from pytest import fixture

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
    provide_targeted_factory,
)
from airflow.utils import db

DEFAULT_RESOURCE_GROUP = "defaultResourceGroup"
RESOURCE_GROUP = "testResourceGroup"

DEFAULT_FACTORY = "defaultFactory"
FACTORY = "testFactory"

DEFAULT_CONNECTION_CLIENT_SECRET = "azure_data_factory_test_client_secret"
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
                "extra__azure_data_factory__tenantId": "tenantId",
                "extra__azure_data_factory__subscriptionId": "subscriptionId",
                "extra__azure_data_factory__resource_group_name": DEFAULT_RESOURCE_GROUP,
                "extra__azure_data_factory__factory_name": DEFAULT_FACTORY,
            }
        ),
    )
    connection_default_credential = Connection(
        conn_id=DEFAULT_CONNECTION_DEFAULT_CREDENTIAL,
        conn_type="azure_data_factory",
        extra=json.dumps(
            {
                "extra__azure_data_factory__subscriptionId": "subscriptionId",
                "extra__azure_data_factory__resource_group_name": DEFAULT_RESOURCE_GROUP,
                "extra__azure_data_factory__factory_name": DEFAULT_FACTORY,
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
                "extra__azure_data_factory__tenantId": "tenantId",
                "extra__azure_data_factory__resource_group_name": DEFAULT_RESOURCE_GROUP,
                "extra__azure_data_factory__factory_name": DEFAULT_FACTORY,
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
                "extra__azure_data_factory__subscriptionId": "subscriptionId",
                "extra__azure_data_factory__resource_group_name": DEFAULT_RESOURCE_GROUP,
                "extra__azure_data_factory__factory_name": DEFAULT_FACTORY,
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
        "extra__azure_data_factory__resource_group_name": DEFAULT_RESOURCE_GROUP,
        "extra__azure_data_factory__factory_name": DEFAULT_FACTORY,
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
