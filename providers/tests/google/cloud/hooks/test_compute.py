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

from unittest import mock
from unittest.mock import PropertyMock

import pytest
from google.api_core.retry import Retry

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook, GceOperationStatus

from providers.tests.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

GCE_ZONE = "zone"
GCE_INSTANCE = "instance"
GCE_INSTANCE_TEMPLATE = "instance-template"
GCE_REQUEST_ID = "request_id"
GCE_INSTANCE_GROUP_MANAGER = "instance_group_manager"

PROJECT_ID = "project-id"
RESOURCE_ID = "resource-id"
BODY = {"body": "test"}
ZONE = "zone"
SOURCE_INSTANCE_TEMPLATE = "source-instance-template"
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]

GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3@google.com"]
API_VERSION = "v1"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
COMPUTE_ENGINE_HOOK_PATH = "airflow.providers.google.cloud.hooks.compute.{}"


class TestGcpComputeHookApiCall:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            ComputeEngineHook(api_version=API_VERSION, gcp_conn_id=GCP_CONN_ID, delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = ComputeEngineHook(
                gcp_conn_id=GCP_CONN_ID,
                api_version=API_VERSION,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_insert_template_should_execute_successfully(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance_template(
            project_id=PROJECT_ID,
            body=BODY,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_template_resource=BODY,
                project=PROJECT_ID,
                request_id=None,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_insert_template_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance_template(
            body=BODY,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_template_resource=BODY,
                request_id=None,
                project="mocked-google",
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_delete_template_should_execute_successfully(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance_template(
            project_id=PROJECT_ID,
            resource_id=RESOURCE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance_template=RESOURCE_ID,
                project=PROJECT_ID,
                request_id=None,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_delete_template_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance_template(
            resource_id=RESOURCE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance_template=RESOURCE_ID,
                project="mocked-google",
                request_id=None,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_get_template_should_execute_successfully(self, mock_client):
        self.hook.get_instance_template(
            project_id=PROJECT_ID,
            resource_id=RESOURCE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance_template=RESOURCE_ID,
                project=PROJECT_ID,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_template_client"))
    def test_get_template_should_not_throw_ex_when_project_id_none(self, mock_client, mocked_project_id):
        self.hook.get_instance_template(
            resource_id=RESOURCE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance_template=RESOURCE_ID,
                project="mocked-google",
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_insert_instance_should_execute_successfully(self, mock_client, wait_for_operation_to_complete):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance(
            project_id=PROJECT_ID,
            body=BODY,
            zone=ZONE,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_resource=BODY,
                project=PROJECT_ID,
                request_id=None,
                zone=ZONE,
                source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_insert_instance_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance(
            body=BODY,
            zone=ZONE,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_resource=BODY,
                project="mocked-google",
                request_id=None,
                zone=ZONE,
                source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_get_instance_should_execute_successfully(self, mock_client):
        self.hook.get_instance(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance=RESOURCE_ID,
                project=PROJECT_ID,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_get_instance_should_not_throw_ex_when_project_id_none(self, mock_client, mocked_project_id):
        self.hook.get_instance(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance=RESOURCE_ID,
                project="mocked-google",
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_delete_instance_should_execute_successfully(self, mock_client, wait_for_operation_to_complete):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance=RESOURCE_ID,
                project=PROJECT_ID,
                request_id=None,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_client"))
    def test_delete_instance_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance=RESOURCE_ID,
                project="mocked-google",
                request_id=None,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_insert_instance_group_manager_should_execute_successfully(
        self, mock_client, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance_group_manager(
            body=BODY,
            zone=ZONE,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_group_manager_resource=BODY,
                project=PROJECT_ID,
                request_id=None,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_insert_instance_group_manager_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.insert_instance_group_manager(
            body=BODY,
            zone=ZONE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.insert.assert_called_once_with(
            request=dict(
                instance_group_manager_resource=BODY,
                project="mocked-google",
                request_id=None,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_get_instance_group_manager_should_execute_successfully(self, mock_client):
        self.hook.get_instance_group_manager(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance_group_manager=RESOURCE_ID,
                project=PROJECT_ID,
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_get_instance_group_manager_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id
    ):
        self.hook.get_instance_group_manager(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.get.assert_called_once_with(
            request=dict(
                instance_group_manager=RESOURCE_ID,
                project="mocked-google",
                zone=ZONE,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_delete_instance_group_manager_should_execute_successfully(
        self, mock_client, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance_group_manager(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance_group_manager=RESOURCE_ID,
                project=PROJECT_ID,
                zone=ZONE,
                request_id=None,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook._wait_for_operation_to_complete"))
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value="mocked-google",
    )
    @mock.patch(
        COMPUTE_ENGINE_HOOK_PATH.format("ComputeEngineHook.get_compute_instance_group_managers_client")
    )
    def test_delete_instance_group_manager_should_not_throw_ex_when_project_id_none(
        self, mock_client, mocked_project_id, wait_for_operation_to_complete
    ):
        wait_for_operation_to_complete.return_value = None
        self.hook.delete_instance_group_manager(
            resource_id=RESOURCE_ID,
            zone=ZONE,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_client.return_value.delete.assert_called_once_with(
            request=dict(
                instance_group_manager=RESOURCE_ID,
                project="mocked-google",
                zone=ZONE,
                request_id=None,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestGcpComputeHookNoDefaultProjectId:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gce_hook_no_project_id = ComputeEngineHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.compute.build")
    def test_gce_client_creation(self, mock_build, mock_authorize):
        result = self.gce_hook_no_project_id.get_conn()
        mock_build.assert_called_once_with(
            "compute", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_start_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        start_method = get_conn.return_value.instances.return_value.start
        execute_method = start_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook_no_project_id.start_instance(
            project_id="example-project", zone=GCE_ZONE, resource_id=GCE_INSTANCE
        )
        assert res is None
        start_method.assert_called_once_with(instance="instance", project="example-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_stop_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        stop_method = get_conn.return_value.instances.return_value.stop
        execute_method = stop_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook_no_project_id.stop_instance(
            project_id="example-project", zone=GCE_ZONE, resource_id=GCE_INSTANCE
        )
        assert res is None
        stop_method.assert_called_once_with(instance="instance", project="example-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_set_machine_type_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        set_machine_type_method = get_conn.return_value.instances.return_value.setMachineType
        execute_method = set_machine_type_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook_no_project_id.set_machine_type(
            body={}, project_id="example-project", zone=GCE_ZONE, resource_id=GCE_INSTANCE
        )
        assert res is None
        set_machine_type_method.assert_called_once_with(
            body={}, instance="instance", project="example-project", zone="zone"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_patch_instance_group_manager_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn
    ):
        patch_method = get_conn.return_value.instanceGroupManagers.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook_no_project_id.patch_instance_group_manager(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER,
            body={},
            request_id=GCE_REQUEST_ID,
        )
        assert res is None
        patch_method.assert_called_once_with(
            body={},
            instanceGroupManager="instance_group_manager",
            project="example-project",
            requestId="request_id",
            zone="zone",
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project", zone="zone"
        )


class TestGcpComputeHookDefaultProjectId:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gce_hook = ComputeEngineHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_start_instance(self, wait_for_operation_to_complete, get_conn, mock_project_id):
        start_method = get_conn.return_value.instances.return_value.start
        execute_method = start_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.start_instance(
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        assert res is None
        start_method.assert_called_once_with(instance="instance", project="example-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_start_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        start_method = get_conn.return_value.instances.return_value.start
        execute_method = start_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.start_instance(project_id="new-project", zone=GCE_ZONE, resource_id=GCE_INSTANCE)
        assert res is None
        start_method.assert_called_once_with(instance="instance", project="new-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="new-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_stop_instance(self, wait_for_operation_to_complete, get_conn, mock_project_id):
        stop_method = get_conn.return_value.instances.return_value.stop
        execute_method = stop_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.stop_instance(
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        assert res is None
        stop_method.assert_called_once_with(instance="instance", project="example-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_stop_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        stop_method = get_conn.return_value.instances.return_value.stop
        execute_method = stop_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.stop_instance(project_id="new-project", zone=GCE_ZONE, resource_id=GCE_INSTANCE)
        assert res is None
        stop_method.assert_called_once_with(instance="instance", project="new-project", zone="zone")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="new-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_set_machine_type_instance(self, wait_for_operation_to_complete, get_conn, mock_project_id):
        execute_method = get_conn.return_value.instances.return_value.setMachineType.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.set_machine_type(
            body={},
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        assert res is None
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_set_machine_type_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        execute_method = get_conn.return_value.instances.return_value.setMachineType.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.set_machine_type(
            project_id="new-project", body={}, zone=GCE_ZONE, resource_id=GCE_INSTANCE
        )
        assert res is None
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="new-project", operation_name="operation_id", zone="zone"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_patch_instance_group_manager(self, wait_for_operation_to_complete, get_conn, mock_project_id):
        patch_method = get_conn.return_value.instanceGroupManagers.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.patch_instance_group_manager(
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER,
            body={},
            request_id=GCE_REQUEST_ID,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        assert res is None
        patch_method.assert_called_once_with(
            body={},
            instanceGroupManager="instance_group_manager",
            project="example-project",
            requestId="request_id",
            zone="zone",
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._wait_for_operation_to_complete"
    )
    def test_patch_instance_group_manager_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn
    ):
        patch_method = get_conn.return_value.instanceGroupManagers.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gce_hook.patch_instance_group_manager(
            project_id="new-project",
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER,
            body={},
            request_id=GCE_REQUEST_ID,
        )
        assert res is None
        patch_method.assert_called_once_with(
            body={},
            instanceGroupManager="instance_group_manager",
            project="new-project",
            requestId="request_id",
            zone="zone",
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="new-project", zone="zone"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._check_global_operation_status"
    )
    def test_wait_for_operation_to_complete_no_zone(self, mock_operation_status, mock_get_conn):
        service = "test-service"
        project_id = "test-project"
        operation_name = "test-operation"
        num_retries = self.gce_hook.num_retries

        # Test success
        mock_get_conn.return_value = service
        mock_operation_status.return_value = {"status": GceOperationStatus.DONE, "error": None}
        self.gce_hook._wait_for_operation_to_complete(
            project_id=project_id, operation_name=operation_name, zone=None
        )

        mock_operation_status.assert_called_once_with(
            service=service, operation_name=operation_name, project_id=project_id, num_retries=num_retries
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._check_global_operation_status"
    )
    def test_wait_for_operation_to_complete_no_zone_error(self, mock_operation_status, mock_get_conn):
        service = "test-service"
        project_id = "test-project"
        operation_name = "test-operation"

        # Test error
        mock_get_conn.return_value = service
        mock_operation_status.return_value = {
            "status": GceOperationStatus.DONE,
            "error": {"errors": "some nasty errors"},
            "httpErrorStatusCode": 400,
            "httpErrorMessage": "sample msg",
        }

        with pytest.raises(AirflowException):
            self.gce_hook._wait_for_operation_to_complete(
                project_id=project_id, operation_name=operation_name, zone=None
            )

    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.compute.ComputeEngineHook._check_zone_operation_status")
    def test_wait_for_operation_to_complete_with_zone(self, mock_operation_status, mock_get_conn):
        service = "test-service"
        project_id = "test-project"
        operation_name = "test-operation"
        zone = "west-europe3"
        num_retries = self.gce_hook.num_retries

        # Test success
        mock_get_conn.return_value = service
        mock_operation_status.return_value = {"status": GceOperationStatus.DONE, "error": None}
        self.gce_hook._wait_for_operation_to_complete(
            project_id=project_id, operation_name=operation_name, zone=zone
        )

        mock_operation_status.assert_called_once_with(service, operation_name, project_id, zone, num_retries)
