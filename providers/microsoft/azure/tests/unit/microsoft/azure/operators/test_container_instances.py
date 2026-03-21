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
from __future__ import annotations

from collections import namedtuple
from collections.abc import MutableMapping
from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import pytest
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerPropertiesInstanceView,
    ContainerState,
    Event,
)

from airflow.exceptions import TaskDeferred
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
from airflow.providers.microsoft.azure.triggers.container_instance import AzureContainerInstanceTrigger

from tests_common.test_utils.compat import Context


def make_mock_cg(container_state, events=None):
    """
    Make a mock Container Group as the underlying azure Models have read-only attributes
    See https://docs.microsoft.com/en-us/rest/api/container-instances/containergroups
    """
    events = events or []
    instance_view_dict = {"current_state": container_state, "events": events}
    instance_view = namedtuple("ContainerPropertiesInstanceView", instance_view_dict.keys())(
        *instance_view_dict.values()
    )

    container_dict = {"instance_view": instance_view}
    container = namedtuple("Containers", container_dict.keys())(*container_dict.values())

    container_g_dict = {"containers": [container]}
    container_g = namedtuple("ContainerGroup", container_g_dict.keys())(*container_g_dict.values())
    return container_g


def make_mock_cg_with_missing_events(container_state):
    """
    Make a mock Container Group as the underlying azure Models have read-only attributes
    See https://docs.microsoft.com/en-us/rest/api/container-instances/containergroups
    This creates the Container Group without events.
    This can happen, when the container group is provisioned, but not started.
    """
    instance_view_dict = {"current_state": container_state, "events": None}
    instance_view = namedtuple("ContainerPropertiesInstanceView", instance_view_dict.keys())(
        *instance_view_dict.values()
    )

    container_dict = {"instance_view": instance_view}
    container = namedtuple("Containers", container_dict.keys())(*container_dict.values())

    container_g_dict = {"containers": [container]}
    container_g = namedtuple("ContainerGroup", container_g_dict.keys())(*container_g_dict.values())
    return container_g


# TODO: FIXME the assignment here seem wrong byt they do work in these mocks - should likely be better done


def make_mock_container(state: str, exit_code: int, detail_status: str, events: Event | None = None):
    container = Container(name="hello_world", image="test", resources="test")  # type: ignore[arg-type]
    container_prop = ContainerPropertiesInstanceView()
    container_state = ContainerState()
    container_state.state = state  # type: ignore[assignment]
    container_state.exit_code = exit_code  # type: ignore[assignment]
    container_state.detail_status = detail_status  # type: ignore[assignment]
    container_prop.current_state = container_state  # type: ignore[assignment]
    if events:
        container_prop.events = events  # type: ignore[assignment]
    container.instance_view = container_prop  # type: ignore[assignment]

    cg = ContainerGroup(containers=[container], os_type="Linux")

    return cg


class TestACIOperator:
    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute(self, aci_mock):
        expected_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")

        aci_mock.return_value.get_state.return_value = expected_cg

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            remove_on_error=False,
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_rg == "resource-group"
        assert called_cn == "container-name"

        assert called_cg.location == "region"
        assert called_cg.image_registry_credentials is None
        assert called_cg.restart_policy == "Never"
        assert called_cg.os_type == "Linux"

        called_cg_container = called_cg.containers[0]
        assert called_cg_container.name == "container-name"
        assert called_cg_container.image == "container-image"

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_failures(self, aci_mock):
        expected_cg = make_mock_container(state="Terminated", exit_code=1, detail_status="test")
        aci_mock.return_value.get_state.return_value = expected_cg

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
        )
        with pytest.raises(AirflowException):
            aci.execute(None)

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    @pytest.mark.parametrize("state", ["Terminated", "Unhealthy"])
    def test_execute_with_failures_without_removal(self, aci_mock, state):
        expected_cg = make_mock_container(state=state, exit_code=1, detail_status="test")
        aci_mock.return_value.get_state.return_value = expected_cg

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            remove_on_error=False,
        )
        with pytest.raises(AirflowException):
            aci.execute(None)

        assert aci_mock.return_value.delete.call_count == 0

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_tags(self, aci_mock):
        expected_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = expected_cg
        tags = {"testKey": "testValue"}

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            tags=tags,
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_rg == "resource-group"
        assert called_cn == "container-name"

        assert called_cg.location == "region"
        assert called_cg.image_registry_credentials is None
        assert called_cg.restart_policy == "Never"
        assert called_cg.os_type == "Linux"
        assert called_cg.tags == tags

        called_cg_container = called_cg.containers[0]
        assert called_cg_container.name == "container-name"
        assert called_cg_container.image == "container-image"

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_messages_logs(self, aci_mock):
        event1 = Event()
        event1.message = "test"
        event2 = Event()
        event2.message = "messages"
        events = [event1, event2]
        expected_cg1 = make_mock_container(
            state="Succeeded", exit_code=0, detail_status="test", events=events
        )
        expected_cg2 = make_mock_container(state="Running", exit_code=0, detail_status="test", events=events)
        expected_cg3 = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test", events=events
        )

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2, expected_cg3]
        aci_mock.return_value.get_logs.return_value = ["test", "logs"]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        assert aci_mock.return_value.get_state.call_count == 3
        assert aci_mock.return_value.get_logs.call_count == 3

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_messages_all_logs_in_xcom_logs(self, aci_mock):
        event1 = Event()
        event1.message = "test"
        event2 = Event()
        event2.message = "messages"
        events = [event1, event2]
        expected_cg1 = make_mock_container(
            state="Succeeded", exit_code=0, detail_status="test", events=events
        )
        expected_cg2 = make_mock_container(state="Running", exit_code=0, detail_status="test", events=events)
        expected_cg3 = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test", events=events
        )

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2, expected_cg3]
        aci_mock.return_value.get_logs.return_value = ["test", "logs"]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            xcom_all=True,
        )
        context = Context(ti=XcomMock())
        aci.execute(context)

        assert aci_mock.return_value.create_or_update.call_count == 1
        assert aci_mock.return_value.get_state.call_count == 3
        assert aci_mock.return_value.get_logs.call_count == 4

        assert aci_mock.return_value.delete.call_count == 1
        assert context["ti"].xcom_pull(key="logs") == aci_mock.return_value.get_logs.return_value

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_messages_last_log_in_xcom_logs(self, aci_mock):
        event1 = Event()
        event1.message = "test"
        event2 = Event()
        event2.message = "messages"
        events = [event1, event2]
        expected_cg1 = make_mock_container(
            state="Succeeded", exit_code=0, detail_status="test", events=events
        )
        expected_cg2 = make_mock_container(state="Running", exit_code=0, detail_status="test", events=events)
        expected_cg3 = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test", events=events
        )

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2, expected_cg3]
        aci_mock.return_value.get_logs.return_value = ["test", "logs"]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            xcom_all=False,
        )
        context = Context(ti=XcomMock())
        aci.execute(context)

        assert aci_mock.return_value.create_or_update.call_count == 1
        assert aci_mock.return_value.get_state.call_count == 3
        assert aci_mock.return_value.get_logs.call_count == 4

        assert aci_mock.return_value.delete.call_count == 1
        assert context["ti"].xcom_pull(key="logs") == aci_mock.return_value.get_logs.return_value[-1:]
        assert context["ti"].xcom_pull(key="logs") == ["logs"]

    def test_name_checker(self):
        valid_names = ["test-dash", "name-with-length---63" * 3]

        invalid_names = [
            "test_underscore",
            "name-with-length---84" * 4,
            "name-ending-with-dash-",
            "-name-starting-with-dash",
        ]
        for name in invalid_names:
            with pytest.raises(AirflowException):
                AzureContainerInstancesOperator._check_name(name)

        for name in valid_names:
            checked_name = AzureContainerInstancesOperator._check_name(name)
            assert checked_name == name

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_ipaddress(self, aci_mock):
        ipaddress = MagicMock()

        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            ip_address=ipaddress,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.ip_address == ipaddress

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_windows_os_and_diff_restart_policy(self, aci_mock):
        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            restart_policy="Always",
            os_type="Windows",
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.restart_policy == "Always"
        assert called_cg.os_type == "Windows"

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_fails_with_incorrect_os_type(self, aci_mock):
        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        with pytest.raises(AirflowException) as ctx:
            AzureContainerInstancesOperator(
                ci_conn_id=None,
                registry_conn_id=None,
                resource_group="resource-group",
                name="container-name",
                image="container-image",
                region="region",
                task_id="task",
                os_type="MacOs",
            )

        assert (
            str(ctx.value) == "Invalid value for the os_type argument. "
            "Please set 'Linux' or 'Windows' as the os_type. "
            "Found `MacOs`."
        )

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_fails_with_incorrect_restart_policy(self, aci_mock):
        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        with pytest.raises(AirflowException) as ctx:
            AzureContainerInstancesOperator(
                ci_conn_id=None,
                registry_conn_id=None,
                resource_group="resource-group",
                name="container-name",
                image="container-image",
                region="region",
                task_id="task",
                restart_policy="Everyday",
            )

        assert (
            str(ctx.value) == "Invalid value for the restart_policy argument. "
            "Please set one of 'Always', 'OnFailure','Never' as the restart_policy. "
            "Found `Everyday`"
        )

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.time.sleep")
    def test_execute_correct_sleep_cycle(self, sleep_mock, aci_mock):
        expected_cg1 = make_mock_container(state="Running", exit_code=0, detail_status="test")
        expected_cg2 = make_mock_container(state="Terminated", exit_code=0, detail_status="test")

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg1, expected_cg2]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
        )
        aci.execute(None)

        # sleep is called at the end of cycles. Thus, the Terminated call does not trigger sleep
        assert sleep_mock.call_count == 2

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    @mock.patch("logging.Logger.exception")
    def test_execute_with_missing_events(self, log_mock, aci_mock):
        expected_cg1 = make_mock_container(state="Running", exit_code=0, detail_status="test")
        expected_cg2 = make_mock_container(state="Terminated", exit_code=0, detail_status="test")

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
        )

        aci.execute(None)

        assert log_mock.call_count == 0

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_dnsconfig(self, aci_mock):
        dns_config = MagicMock()

        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            dns_config=dns_config,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.dns_config == dns_config

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_diagnostics(self, aci_mock):
        diagnostics = MagicMock()

        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            diagnostics=diagnostics,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.diagnostics == diagnostics

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_spot_discount(self, aci_mock):
        expected_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = expected_cg

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            priority="Spot",
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_rg == "resource-group"
        assert called_cn == "container-name"

        assert called_cg.location == "region"
        assert called_cg.image_registry_credentials is None
        assert called_cg.restart_policy == "Never"
        assert called_cg.os_type == "Linux"
        assert called_cg.priority == "Spot"

        called_cg_container = called_cg.containers[0]
        assert called_cg_container.name == "container-name"
        assert called_cg_container.image == "container-image"

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_identity(self, aci_mock):
        identity = MagicMock()

        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            identity=identity,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.identity == identity

    @mock.patch("airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook")
    def test_execute_with_identity_dict(self, aci_mock):
        # New test: pass a dict and verify operator converts it to ContainerGroupIdentity
        resource_id = "/subscriptions/00000000-0000-0000-0000-00000000000/resourceGroups/my_rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/my_identity"
        identity_dict = {
            "type": "UserAssigned",
            "resource_ids": [resource_id],
        }

        aci_mock.return_value.get_state.return_value = make_mock_container(
            state="Terminated", exit_code=0, detail_status="test"
        )

        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            identity=identity_dict,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        # verify the operator converted dict -> ContainerGroupIdentity with proper mapping
        assert hasattr(called_cg, "identity")
        assert called_cg.identity is not None
        # user_assigned_identities should contain the resource id as a key
        assert resource_id in (called_cg.identity.user_assigned_identities or {})

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_deferrable_defers_when_container_running(self, aci_mock):
        """When deferrable=True and the container is still running, defer() is called."""
        running_cg = make_mock_container(state="Running", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = running_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            polling_interval=10.0,
        )
        with pytest.raises(TaskDeferred) as exc_info:
            aci.execute(None)

        assert isinstance(exc_info.value.trigger, AzureContainerInstanceTrigger)
        assert exc_info.value.trigger.resource_group == "resource-group"
        assert exc_info.value.trigger.name == "container-name"
        assert exc_info.value.trigger.polling_interval == 10.0
        assert exc_info.value.method_name == "execute_complete"
        # Container must NOT be deleted when deferring — it is still running
        assert aci_mock.return_value.delete.call_count == 0

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_deferrable_completes_synchronously_if_already_terminated(self, aci_mock):
        """When deferrable=True but container is already terminal, no deferral — sync completion."""
        terminated_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = terminated_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
        )
        result = aci.execute(None)
        assert result == 0
        assert aci_mock.return_value.create_or_update.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_success(self, aci_mock):
        """execute_complete succeeds, does not raise, and deletes the container group."""
        aci_mock.return_value.get_logs.return_value = None

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            remove_on_success=True,
        )
        result = aci.execute_complete(
            context=None,
            event={
                "status": "success",
                "exit_code": 0,
                "detail_status": "Completed",
                "resource_group": "resource-group",
                "name": "container-name",
            },
        )
        assert result == 0
        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_success_with_remove_on_success_false(self, aci_mock):
        """execute_complete with remove_on_success=False should NOT delete the container."""
        aci_mock.return_value.get_logs.return_value = None

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            remove_on_success=False,
        )
        aci.execute_complete(
            context=None,
            event={
                "status": "success",
                "exit_code": 0,
                "detail_status": "Completed",
                "resource_group": "resource-group",
                "name": "container-name",
            },
        )
        assert aci_mock.return_value.delete.call_count == 0

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_error_event_raises(self, aci_mock):
        """execute_complete raises AirflowException when event has status=error."""
        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            remove_on_error=False,
        )
        with pytest.raises(RuntimeError, match="Container group failed"):
            aci.execute_complete(
                context=None,
                event={
                    "status": "error",
                    "exit_code": 1,
                    "detail_status": "OOMKilled",
                    "message": "Container group failed with exit code 1",
                    "resource_group": "resource-group",
                    "name": "container-name",
                },
            )

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_error_event_removes_on_error(self, aci_mock):
        """execute_complete with remove_on_error=True deletes the container on error."""
        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            remove_on_error=True,
        )
        with pytest.raises(RuntimeError):
            aci.execute_complete(
                context=None,
                event={
                    "status": "error",
                    "exit_code": 1,
                    "detail_status": "Failed",
                    "message": "Container group failed with exit code 1",
                    "resource_group": "resource-group",
                    "name": "container-name",
                },
            )
        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_none_event_raises(self, aci_mock):
        """execute_complete raises ValueError when event is None."""
        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
        )
        with pytest.raises(ValueError, match="event is None"):
            aci.execute_complete(context=None, event=None)

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_pushes_all_logs_to_xcom(self, aci_mock):
        """execute_complete pushes all logs to XCom when xcom_all=True."""
        aci_mock.return_value.get_logs.return_value = ["line1\n", "line2\n"]

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            xcom_all=True,
            remove_on_success=False,
        )
        context = Context(ti=XcomMock())
        aci.execute_complete(
            context=context,
            event={
                "status": "success",
                "exit_code": 0,
                "detail_status": "Completed",
                "resource_group": "resource-group",
                "name": "container-name",
            },
        )
        assert context["ti"].xcom_pull(key="logs") == ["line1\n", "line2\n"]

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_complete_pushes_last_log_to_xcom(self, aci_mock):
        """execute_complete pushes only last log line to XCom when xcom_all=False."""
        aci_mock.return_value.get_logs.return_value = ["line1\n", "line2\n"]

        aci = AzureContainerInstancesOperator(
            ci_conn_id="azure_default",
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            deferrable=True,
            xcom_all=False,
            remove_on_success=False,
        )
        context = Context(ti=XcomMock())
        aci.execute_complete(
            context=context,
            event={
                "status": "success",
                "exit_code": 0,
                "detail_status": "Completed",
                "resource_group": "resource-group",
                "name": "container-name",
            },
        )
        assert context["ti"].xcom_pull(key="logs") == ["line2\n"]

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_remove_on_success_false_sync_path(self, aci_mock):
        """Non-deferrable: remove_on_success=False should NOT delete the container on success."""
        terminated_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = terminated_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            remove_on_success=False,
        )
        aci.execute(None)
        assert aci_mock.return_value.delete.call_count == 0

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstanceHook",
        autospec=True,
    )
    def test_execute_remove_on_success_true_sync_path(self, aci_mock):
        """Non-deferrable: remove_on_success=True (default) deletes the container on success."""
        terminated_cg = make_mock_container(state="Terminated", exit_code=0, detail_status="test")
        aci_mock.return_value.get_state.return_value = terminated_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group="resource-group",
            name="container-name",
            image="container-image",
            region="region",
            task_id="task",
            remove_on_success=True,
        )
        aci.execute(None)
        assert aci_mock.return_value.delete.call_count == 1


class XcomMock:
    def __init__(self) -> None:
        self.values: MutableMapping[str, Any | None] = {}

    def xcom_push(self, key: str, value: Any | None) -> None:
        self.values[key] = value

    def xcom_pull(self, key: str) -> Any:
        return self.values[key]
