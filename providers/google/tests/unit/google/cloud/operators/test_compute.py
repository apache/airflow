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

import ast
from copy import deepcopy
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.compute_v1.types import Instance, InstanceGroupManager, InstanceTemplate

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineDeleteInstanceGroupManagerOperator,
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineDeleteInstanceTemplateOperator,
    ComputeEngineInsertInstanceFromTemplateOperator,
    ComputeEngineInsertInstanceGroupManagerOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineInsertInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils import timezone

EMPTY_CONTENT = b""
GCP_PROJECT_ID = "project-id"
GCE_ZONE = "zone"
GCE_RESOURCE_ID = "resource-id"
GCE_SHORT_MACHINE_TYPE_NAME = "n1-machine-type"
GCE_SOURCE_IMAGE = "projects/debian-cloud/global/images/debian-11-bullseye-v20220621"
GCP_INSTANCE_TEMPLATE_NAME = "instance-template-name"
TASK_ID = "task-id"

SET_MACHINE_TYPE_BODY = {"machineType": f"zones/{GCE_ZONE}/machineTypes/{GCE_SHORT_MACHINE_TYPE_NAME}"}
SOURCE_INSTANCE_TEMPLATE = f"global/InstanceTemplates/{GCP_INSTANCE_TEMPLATE_NAME}"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)

GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]
API_VERSION = "v1"

COMPUTE_ENGINE_HOOK_PATH = "airflow.providers.google.cloud.operators.compute.ComputeEngineHook"
IGM_PATH = "airflow.providers.google.cloud.operators.compute.InstanceGroupManager"

GCP_INSTANCE_BODY_FROM_TEMPLATE = {"name": "a-instance-from-template"}
GCE_INSTANCE_BODY_API_CALL = {
    "name": "instance-name",
    "machine_type": f"zones/{GCE_ZONE}/machineTypes/{GCE_SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": "instance-name",
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{GCE_ZONE}/diskTypes/pd-balanced",
                "source_image": GCE_SOURCE_IMAGE,
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": "regions/us-central1/subnetworks/default",
        }
    ],
}
GCE_INSTANCE_BODY_WITHOUT_NAME_API_CALL = {
    "machine_type": f"zones/{GCE_ZONE}/machineTypes/{GCE_SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": "instance-name",
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{GCE_ZONE}/diskTypes/pd-balanced",
                "source_image": GCE_SOURCE_IMAGE,
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": "regions/us-central1/subnetworks/default",
        }
    ],
}


class TestGceInstanceInsert:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_should_execute_successfully(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        op = ComputeEngineInsertInstanceOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_RESOURCE_ID,
            body=GCE_INSTANCE_BODY_API_CALL,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            api_version=API_VERSION,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_BODY_API_CALL,
            zone=GCE_ZONE,
            request_id=None,
        )

    def test_insert_instance_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineInsertInstanceOperator(
                project_id="",
                body=GCE_INSTANCE_BODY_API_CALL,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_should_not_throw_ex_when_project_id_none(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        op = ComputeEngineInsertInstanceOperator(
            resource_id=GCE_RESOURCE_ID,
            body=GCE_INSTANCE_BODY_API_CALL,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            body=GCE_INSTANCE_BODY_API_CALL,
            zone=GCE_ZONE,
            request_id=None,
            project_id=None,
        )

    def test_insert_instance_should_throw_ex_when_missing_zone(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'zone' is missing"):
            ComputeEngineInsertInstanceOperator(
                resource_id=GCE_RESOURCE_ID,
                body=GCE_INSTANCE_BODY_API_CALL,
                zone="",
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_insert_instance_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(
            AirflowException,
            match=r"The required parameters 'resource_id' and "
            r"body\['name'\] are missing\. Please, provide "
            r"at least one of them",
        ):
            ComputeEngineInsertInstanceOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                body=GCE_INSTANCE_BODY_WITHOUT_NAME_API_CALL,
                task_id=TASK_ID,
                resource_id="",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_should_not_throw_ex_when_name_is_templated(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        body_with_templated_name = deepcopy(GCE_INSTANCE_BODY_API_CALL)
        body_with_templated_name["name"] = "{{ logical_date }}"
        op = ComputeEngineInsertInstanceOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_RESOURCE_ID,
            body=body_with_templated_name,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_with_templated_name,
            zone=GCE_ZONE,
            request_id=None,
        )


class TestGceInstanceInsertFromTemplate:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_from_template_should_execute_successfully(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        op = ComputeEngineInsertInstanceFromTemplateOperator(
            project_id=GCP_PROJECT_ID,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            request_id=None,
        )

    def test_insert_instance_from_template_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineInsertInstanceFromTemplateOperator(
                project_id="",
                source_instance_template=SOURCE_INSTANCE_TEMPLATE,
                body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_from_template_should_not_throw_ex_when_project_id_none(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        op = ComputeEngineInsertInstanceFromTemplateOperator(
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            project_id=None,
            zone=GCE_ZONE,
            body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            request_id=None,
        )

    def test_insert_instance_from_template_should_throw_ex_when_missing_zone(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'zone' is missing"):
            ComputeEngineInsertInstanceFromTemplateOperator(
                project_id=GCP_PROJECT_ID,
                zone="",
                source_instance_template=SOURCE_INSTANCE_TEMPLATE,
                body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_insert_instance_from_template_should_throw_ex_when_missing_source_instance_template(self):
        with pytest.raises(
            (TypeError, AirflowException), match=r"missing keyword argument 'source_instance_template'"
        ):
            ComputeEngineInsertInstanceFromTemplateOperator(
                project_id=GCP_PROJECT_ID,
                body=GCP_INSTANCE_BODY_FROM_TEMPLATE,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_insert_instance_from_template_should_throw_ex_when_missing_body(self):
        with pytest.raises((TypeError, AirflowException), match=r"missing keyword argument 'body'"):
            ComputeEngineInsertInstanceFromTemplateOperator(
                project_id=GCP_PROJECT_ID,
                source_instance_template=SOURCE_INSTANCE_TEMPLATE,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_instance_from_template_should_not_throw_ex_when_name_is_templated(self, mock_hook):
        get_instance_obj_mock = mock.MagicMock()
        get_instance_obj_mock.__class__ = Instance
        mock_hook.return_value.get_instance.side_effect = [
            NotFound("Error message"),
            get_instance_obj_mock,
        ]
        body_with_templated_name = deepcopy(GCP_INSTANCE_BODY_FROM_TEMPLATE)
        body_with_templated_name["name"] = "{{ execution_date }}"
        op = ComputeEngineInsertInstanceFromTemplateOperator(
            project_id=GCP_PROJECT_ID,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            body=body_with_templated_name,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            body=body_with_templated_name,
            source_instance_template=SOURCE_INSTANCE_TEMPLATE,
            request_id=None,
        )


class TestGceInstanceDelete:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_delete_instance_should_execute_successfully(self, mock_hook):
        op = ComputeEngineDeleteInstanceOperator(
            resource_id=GCE_RESOURCE_ID,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            request_id=None,
            resource_id=GCE_RESOURCE_ID,
            zone=GCE_ZONE,
        )

    def test_delete_instance_should_throw_ex_when_missing_zone(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'zone' is missing"):
            ComputeEngineDeleteInstanceOperator(
                resource_id=GCE_RESOURCE_ID,
                zone="",
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_delete_instance_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineDeleteInstanceOperator(
                resource_id="",
                zone=GCE_ZONE,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


class TestGceInstanceStart:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_start_instance_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.start_instance.return_value = True
        op = ComputeEngineStartInstanceOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)

        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.start_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as template dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @pytest.mark.db_test
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_start_instance_with_templates(self, _, create_task_instance_of_operator, session):
        dag_id = "test_instance_start_with_templates"
        ti = create_task_instance_of_operator(
            ComputeEngineStartInstanceOperator,
            dag_id=dag_id,
            project_id="{{ dag.dag_id }}",
            zone="{{ dag.dag_id }}",
            resource_id="{{ dag.dag_id }}",
            gcp_conn_id="{{ dag.dag_id }}",
            api_version="{{ dag.dag_id }}",
            task_id="id",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert dag_id == ti.task.project_id
        assert dag_id == ti.task.zone
        assert dag_id == ti.task.resource_id
        assert dag_id == ti.task.gcp_conn_id
        assert dag_id == ti.task.api_version

    def test_instance_start_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineStartInstanceOperator(
                project_id="",
                zone=GCE_ZONE,
                resource_id=GCE_RESOURCE_ID,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_instance_start_should_not_throw_ex_when_project_id_none(self, _):
        op = ComputeEngineStartInstanceOperator(
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)

    def test_instance_start_should_throw_ex_when_missing_zone(self):
        with pytest.raises(AirflowException, match=r"he required parameter 'zone' is missing"):
            ComputeEngineStartInstanceOperator(
                project_id=GCP_PROJECT_ID,
                zone="",
                resource_id=GCE_RESOURCE_ID,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_instance_start_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineStartInstanceOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id="",
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


class TestGceInstanceStop:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_instance_stop_should_execute_successfully(self, mock_hook):
        op = ComputeEngineStopInstanceOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.stop_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @pytest.mark.db_test
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_instance_stop_with_templates(self, _, create_task_instance_of_operator, session):
        dag_id = "test_instance_stop_with_templates"
        ti = create_task_instance_of_operator(
            ComputeEngineStopInstanceOperator,
            dag_id=dag_id,
            project_id="{{ dag.dag_id }}",
            zone="{{ dag.dag_id }}",
            resource_id="{{ dag.dag_id }}",
            gcp_conn_id="{{ dag.dag_id }}",
            api_version="{{ dag.dag_id }}",
            task_id="id",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert dag_id == ti.task.project_id
        assert dag_id == ti.task.zone
        assert dag_id == ti.task.resource_id
        assert dag_id == ti.task.gcp_conn_id
        assert dag_id == ti.task.api_version

    def test_instance_stop_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineStopInstanceOperator(
                project_id="", zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, task_id="id"
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_instance_stop_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineStopInstanceOperator(zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, task_id="id")
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.stop_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, project_id=None
        )

    def test_instance_stop_should_throw_ex_when_missing_zone(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'zone' is missing"):
            ComputeEngineStopInstanceOperator(
                project_id=GCP_PROJECT_ID, zone="", resource_id=GCE_RESOURCE_ID, task_id="id"
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_instance_stop_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineStopInstanceOperator(
                project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id="", task_id="id"
            )


class TestGceInstanceSetMachineType:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_machine_type_set_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.set_machine_type.return_value = True
        op = ComputeEngineSetMachineTypeOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, body=SET_MACHINE_TYPE_BODY, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @pytest.mark.db_test
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_machine_type_set_with_templates(self, _, create_task_instance_of_operator, session):
        dag_id = "test_set_machine_type_with_templates"
        ti = create_task_instance_of_operator(
            ComputeEngineSetMachineTypeOperator,
            dag_id=dag_id,
            project_id="{{ dag.dag_id }}",
            zone="{{ dag.dag_id }}",
            resource_id="{{ dag.dag_id }}",
            body={},
            gcp_conn_id="{{ dag.dag_id }}",
            api_version="{{ dag.dag_id }}",
            task_id="id",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert dag_id == ti.task.project_id
        assert dag_id == ti.task.zone
        assert dag_id == ti.task.resource_id
        assert dag_id == ti.task.gcp_conn_id
        assert dag_id == ti.task.api_version

    def test_machine_type_set_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineSetMachineTypeOperator(
                project_id="",
                zone=GCE_ZONE,
                resource_id=GCE_RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id=TASK_ID,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_machine_type_set_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineSetMachineTypeOperator(
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            zone=GCE_ZONE, resource_id=GCE_RESOURCE_ID, body=SET_MACHINE_TYPE_BODY, project_id=None
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_machine_type_set_should_throw_ex_when_missing_zone(self, mock_hook):
        with pytest.raises(AirflowException, match=r"The required parameter 'zone' is missing"):
            ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID,
                zone="",
                resource_id=GCE_RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    def test_machine_type_set_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id="",
                body=SET_MACHINE_TYPE_BODY,
                task_id=TASK_ID,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_machine_type_set_should_throw_ex_when_missing_machine_type(self, mock_hook):
        op = ComputeEngineSetMachineTypeOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            body={},
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        with pytest.raises(AirflowException, match=r"The required body field 'machineType' is missing"):
            op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

    MOCK_OP_RESPONSE = (
        "{'kind': 'compute#operation', 'id': '8529919847974922736', "
        "'name': "
        "'operation-1538578207537-577542784f769-7999ab71-94f9ec1d', "
        "'zone': 'https://www.googleapis.com/compute/v1/projects/example"
        "-project/zones/europe-west3-b', 'operationType': "
        "'setMachineType', 'targetLink': "
        "'https://www.googleapis.com/compute/v1/projects/example-project"
        "/zones/europe-west3-b/instances/pa-1', 'targetId': "
        "'2480086944131075860', 'status': 'DONE', 'user': "
        "'service-account@example-project.iam.gserviceaccount.com', "
        "'progress': 100, 'insertTime': '2018-10-03T07:50:07.951-07:00', "
        "'startTime': '2018-10-03T07:50:08.324-07:00', 'endTime': "
        "'2018-10-03T07:50:08.484-07:00', 'error': {'errors': [{'code': "
        "'UNSUPPORTED_OPERATION', 'message': \"Machine type with name "
        "'machine-type-1' does not exist in zone 'europe-west3-b'.\"}]}, "
        "'httpErrorStatusCode': 400, 'httpErrorMessage': 'BAD REQUEST', "
        "'selfLink': "
        "'https://www.googleapis.com/compute/v1/projects/example-project"
        "/zones/europe-west3-b/operations/operation-1538578207537"
        "-577542784f769-7999ab71-94f9ec1d'} "
    )

    @pytest.mark.db_test
    @mock.patch(
        "airflow.providers.google.cloud.operators.compute.ComputeEngineHook._check_zone_operation_status"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.compute.ComputeEngineHook._execute_set_machine_type"
    )
    @mock.patch("airflow.providers.google.cloud.operators.compute.ComputeEngineHook.get_conn")
    def test_machine_type_set_should_handle_and_trim_gce_error(
        self, get_conn, _execute_set_machine_type, _check_zone_operation_status
    ):
        get_conn.return_value = {}
        _execute_set_machine_type.return_value = {"name": "test-operation"}
        _check_zone_operation_status.return_value = ast.literal_eval(self.MOCK_OP_RESPONSE)
        op = ComputeEngineSetMachineTypeOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id=TASK_ID,
        )
        context = mock.MagicMock()
        with pytest.raises(AirflowException, match=r"400 BAD REQUEST: {.+UNSUPPORTED_OPERATION"):
            op.execute(context=context)
        _check_zone_operation_status.assert_called_once_with(
            {}, "test-operation", GCP_PROJECT_ID, GCE_ZONE, mock.ANY
        )
        _execute_set_machine_type.assert_called_once_with(
            GCE_ZONE, GCE_RESOURCE_ID, SET_MACHINE_TYPE_BODY, GCP_PROJECT_ID
        )


GCE_INSTANCE_TEMPLATE_NAME_API_CALL = "instance-template-test-api"
GCE_INSTANCE_TEMPLATE_BODY_API_CALL = {
    "name": GCE_INSTANCE_TEMPLATE_NAME_API_CALL,
    "properties": {
        "machine_type": GCE_SHORT_MACHINE_TYPE_NAME,
        "disks": [
            {
                "auto_delete": True,
                "boot": True,
                "device_name": GCE_INSTANCE_TEMPLATE_NAME_API_CALL,
                "initialize_params": {
                    "disk_size_gb": "10",
                    "disk_type": "pd-balanced",
                    "source_image": GCE_SOURCE_IMAGE,
                },
            }
        ],
        "network_interfaces": [{"network": "/global/networks/default"}],
    },
}


class TestGceTemplateInsert:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_template_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
        ]
        op = ComputeEngineInsertInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_API_CALL,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_API_CALL,
            request_id=None,
        )

    def test_insert_template_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineInsertInstanceTemplateOperator(
                project_id="",
                body=GCE_INSTANCE_TEMPLATE_BODY_API_CALL,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_template_should_not_throw_ex_when_project_id_none(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
        ]
        op = ComputeEngineInsertInstanceTemplateOperator(
            body=GCE_INSTANCE_TEMPLATE_BODY_API_CALL,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=None,
            body=GCE_INSTANCE_TEMPLATE_BODY_API_CALL,
            request_id=None,
        )

    def test_insert_template_should_throw_ex_when_missing_body(self):
        with pytest.raises((TypeError, AirflowException), match=r"missing keyword argument 'body'"):
            ComputeEngineInsertInstanceTemplateOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_template_should_not_throw_ex_when_name_is_templated(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
        ]
        body_with_templated_name = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_API_CALL)
        body_with_templated_name["name"] = "{{ execution_date }}"
        op = ComputeEngineInsertInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            body=body_with_templated_name,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_with_templated_name,
            request_id=None,
        )


class TestGceTemplateDelete:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_delete_template_should_execute_successfully(self, mock_hook):
        op = ComputeEngineDeleteInstanceTemplateOperator(
            resource_id=GCE_RESOURCE_ID,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            request_id=None,
            resource_id=GCE_RESOURCE_ID,
        )

    def test_delete_template_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineDeleteInstanceTemplateOperator(
                project_id="",
                resource_id=GCE_RESOURCE_ID,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_delete_template_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineDeleteInstanceTemplateOperator(
            resource_id=GCE_RESOURCE_ID,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance_template.assert_called_once_with(
            resource_id=GCE_RESOURCE_ID,
            project_id=None,
            request_id=None,
        )

    def test_delete_template_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineDeleteInstanceTemplateOperator(
                resource_id="",
                project_id=GCP_PROJECT_ID,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


GCE_INSTANCE_TEMPLATE_NAME = "instance-template-test"
GCE_INSTANCE_TEMPLATE_NEW_NAME = "instance-template-test-new"
GCE_INSTANCE_TEMPLATE_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"
GCE_INSTANCE_TEMPLATE_BODY_GET = {
    "kind": "compute#instanceTemplate",
    "id": "6950321349997439715",
    "creation_timestamp": "2018-10-15T06:20:12.777-07:00",
    "name": GCE_INSTANCE_TEMPLATE_NAME,
    "description": "",
    "properties": {
        "machine_type": "n1-standard-1",
        "network_interfaces": [
            {
                "kind": "compute#networkInterface",
                "network": "https://www.googleapis.com/compute/v1/projects/project/global/networks/default",
                "access_configs": [
                    {
                        "kind": "compute#accessConfig",
                        "type": "ONE_TO_ONE_NAT",
                    }
                ],
            },
            {
                "network": "https://www.googleapis.com/compute/v1/projects/project/global/networks/default",
                "access_configs": [{"kind": "compute#accessConfig", "networkTier": "PREMIUM"}],
            },
        ],
        "disks": [
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "A String",
                ],
            }
        ],
    },
    "self_link": "https://www.googleapis.com/compute/v1/projects/project"
    "/global/instanceTemplates/instance-template-test",
}
GCE_INSTANCE_TEMPLATE_BODY_INSERT = {
    "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
}
GCE_INSTANCE_TEMPLATE_BODY_GET_NEW = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_GET)
GCE_INSTANCE_TEMPLATE_BODY_GET_NEW["name"] = GCE_INSTANCE_TEMPLATE_NEW_NAME


class TestGceInstanceTemplateCopy:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID, body=GCE_INSTANCE_TEMPLATE_BODY_INSERT, request_id=None
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_should_not_throw_ex_when_missing_project_id(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=None, body=GCE_INSTANCE_TEMPLATE_BODY_INSERT, request_id=None
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_idempotent_copy_template_when_already_copied_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            get_template_obj_mock,
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME},
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_not_called()

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_with_request_id_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id=TASK_ID,
            body_patch=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_with_description_fields_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id=TASK_ID,
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME, "description": "New description"},
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["description"] = "New description"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_with_some_validation_warnings_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "some_wrong_field": "test",
                "properties": {"some_other_wrong_field": "test"},
            },
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["some_wrong_field"] = "test"
        body_insert["properties"] = {}
        body_insert["properties"]["some_other_wrong_field"] = "test"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=None,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_with_updated_nested_fields_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "machine_type": "n1-standard-2",
                },
            },
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"] = {}
        body_insert["properties"]["machine_type"] = "n1-standard-2"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID, body=body_insert, request_id=None
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_copy_template_with_bigger_array_fields_should_execute_successfully(self, mock_hook):
        get_template_obj_mock = mock.MagicMock()
        get_template_obj_mock.__class__ = InstanceTemplate
        mock_hook.return_value.get_instance_template.side_effect = [
            NotFound("Error message"),
            get_template_obj_mock,
            get_template_obj_mock,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id=TASK_ID,
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "disks": [
                        {
                            "kind": "compute#attachedDisk",
                            "type": "SCRATCH",
                            "licenses": [
                                "Updated String",
                            ],
                        },
                        {
                            "kind": "compute#attachedDisk",
                            "type": "PERSISTENT",
                            "licenses": [
                                "Another String",
                            ],
                        },
                    ],
                },
            },
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            api_version="v1",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"] = {}
        body_insert["properties"]["disks"] = [
            {
                "kind": "compute#attachedDisk",
                "type": "SCRATCH",
                "licenses": [
                    "Updated String",
                ],
            },
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "Another String",
                ],
            },
        ]
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=None,
        )

    def test_copy_template_with_missing_name_should_execute_successfully(self):
        with pytest.raises(
            AirflowException,
            match=r"should contain at least name for the new operator in the 'name' field",
        ):
            ComputeEngineCopyInstanceTemplateOperator(
                project_id=GCP_PROJECT_ID,
                resource_id=GCE_INSTANCE_TEMPLATE_NAME,
                request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
                task_id=TASK_ID,
                body_patch={"description": "New description"},
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


GCE_INSTANCE_GROUP_MANAGER_NAME = "instance-group-test"
GCE_INSTANCE_TEMPLATE_SOURCE_URL = (
    "https://www.googleapis.com/compute/beta/projects/project/global/instanceTemplates/instance-template-test"
)

GCE_INSTANCE_TEMPLATE_OTHER_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-other"
)

GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-non-existing"
)

GCE_INSTANCE_TEMPLATE_DESTINATION_URL = (
    "https://www.googleapis.com/compute/beta/projects/project/global/instanceTemplates/instance-template-new"
)

GCE_INSTANCE_GROUP_MANAGER_GET = {
    "kind": "compute#instanceGroupManager",
    "id": "2822359583810032488",
    "creationTimestamp": "2018-10-17T05:39:35.793-07:00",
    "name": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "zone": "https://www.googleapis.com/compute/beta/projects/project/zones/zone",
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "versions": [
        {"name": "v1", "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL, "targetSize": {"calculated": 1}},
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        },
    ],
    "instanceGroup": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "baseInstanceName": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "fingerprint": "BKWB_igCNbQ=",
    "currentActions": {
        "none": 1,
        "creating": 0,
        "creatingWithoutRetries": 0,
        "verifying": 0,
        "recreating": 0,
        "deleting": 0,
        "abandoning": 0,
        "restarting": 0,
        "refreshing": 0,
    },
    "pendingActions": {"creating": 0, "deleting": 0, "recreating": 0, "restarting": 0},
    "targetSize": 1,
    "selfLink": "https://www.googleapis.com/compute/beta/projects/project/zones/"
    "zone/instanceGroupManagers/" + GCE_INSTANCE_GROUP_MANAGER_NAME,
    "autoHealingPolicies": [{"initialDelaySec": 300}],
    "serviceAccount": "198907790164@cloudservices.gserviceaccount.com",
}

GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {"fixed": 1},
    "maxUnavailable": {"percent": 10},
    "minReadySec": 1800,
}

GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH = {
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
    "versions": [
        {
            "name": "v1",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            "targetSize": {"calculated": 1},
        },
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        },
    ],
    "updatePolicy": GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
}

GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"

GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL = {
    "name": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "base_instance_name": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "instance_template": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "target_size": 1,
}


class TestGceInstanceGroupManagerInsert:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_igm_should_execute_successfully(self, mock_hook):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            NotFound("Error message"),
            get_instance_group_manager_obj_mock,
        ]
        op = ComputeEngineInsertInstanceGroupManagerOperator(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_group_manager.assert_called_once_with(
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL,
            request_id=None,
        )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_igm_should_throw_ex_when_missing_project_id(self, mock_hook):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineInsertInstanceGroupManagerOperator(
                project_id="",
                body=GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_igm_should_not_throw_ex_when_project_id_none(self, mock_hook):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            NotFound("Error message"),
            get_instance_group_manager_obj_mock,
        ]
        op = ComputeEngineInsertInstanceGroupManagerOperator(
            body=GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_group_manager.assert_called_once_with(
            zone=GCE_ZONE,
            project_id=None,
            body=GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL,
            request_id=None,
        )

    def test_insert_igm_should_throw_ex_when_missing_body(self):
        with pytest.raises((TypeError, AirflowException), match=r"missing keyword argument 'body'"):
            ComputeEngineInsertInstanceGroupManagerOperator(
                zone=GCE_ZONE,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_insert_igm_should_not_throw_ex_when_name_is_templated(self, mock_hook):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            NotFound("Error message"),
            get_instance_group_manager_obj_mock,
        ]
        body_with_templated_name = deepcopy(GCE_INSTANCE_GROUP_MANAGER_BODY_API_CALL)
        body_with_templated_name["name"] = "{{ execution_date }}"
        op = ComputeEngineInsertInstanceGroupManagerOperator(
            project_id=GCP_PROJECT_ID,
            body=body_with_templated_name,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.insert_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            body=body_with_templated_name,
            request_id=None,
        )


class TestGceInstanceGroupManagerDelete:
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_delete_igm_should_execute_successfully(self, mock_hook):
        op = ComputeEngineDeleteInstanceGroupManagerOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance_group_manager.assert_called_once_with(
            zone=GCE_ZONE,
            request_id=None,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            project_id=GCP_PROJECT_ID,
        )

    def test_delete_igm_should_throw_ex_when_missing_project_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'project_id' is missing"):
            ComputeEngineDeleteInstanceGroupManagerOperator(
                project_id="",
                resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
                zone=GCE_ZONE,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_delete_igm_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineDeleteInstanceGroupManagerOperator(
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            zone=GCE_ZONE,
            task_id=TASK_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance_group_manager.assert_called_once_with(
            zone=GCE_ZONE,
            request_id=None,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            project_id=None,
        )

    def test_delete_igm_should_throw_ex_when_missing_resource_id(self):
        with pytest.raises(AirflowException, match=r"The required parameter 'resource_id' is missing"):
            ComputeEngineDeleteInstanceGroupManagerOperator(
                resource_id="",
                zone=GCE_ZONE,
                task_id=TASK_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


class TestGceInstanceGroupManagerUpdate:
    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_igm_should_execute_successfully(self, mock_hook, igm):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = GCE_INSTANCE_GROUP_MANAGER_GET

        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=None,
        )
        assert result

    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_missing_project_id_should_execute_successfully(self, mock_hook, igm):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = GCE_INSTANCE_GROUP_MANAGER_GET
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=None,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=None,
        )
        assert result

    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_no_instance_template_field_should_execute_successfully(
        self, mock_hook, igm
    ):
        instance_group_manager_no_template = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_template["instanceTemplate"]
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = instance_group_manager_no_template
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        expected_patch_no_instance_template = deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_instance_template["instanceTemplate"]
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_instance_template,
            request_id=None,
        )
        assert result

    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_no_versions_field_should_execute_successfully(self, mock_hook, igm):
        instance_group_manager_no_versions = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_versions["versions"]
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = instance_group_manager_no_versions
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        expected_patch_no_versions = deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_versions["versions"]
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_versions,
            request_id=None,
        )
        assert result

    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_with_request_id_should_execute_successfully(self, mock_hook, igm):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = GCE_INSTANCE_GROUP_MANAGER_GET
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID,
        )
        assert result

    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_try_to_use_api_v1_should_throw_ex(self, _):
        with pytest.raises(AirflowException, match=r"Use beta api version or above"):
            ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
                task_id=TASK_ID,
                api_version="v1",
                source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
                destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            )

    @mock.patch(IGM_PATH)
    @mock.patch(COMPUTE_ENGINE_HOOK_PATH)
    def test_update_instance_group_try_to_use_non_existing_template_should_not_call_patch(
        self, mock_hook, igm
    ):
        get_instance_group_manager_obj_mock = mock.MagicMock()
        get_instance_group_manager_obj_mock.__class__ = InstanceGroupManager
        mock_hook.return_value.get_instance_group_manager.side_effect = [
            get_instance_group_manager_obj_mock,
        ]
        igm.to_dict.return_value = GCE_INSTANCE_GROUP_MANAGER_GET
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id=TASK_ID,
            source_template=GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="beta",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_not_called()
        assert result
