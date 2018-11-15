# -*- coding: utf-8 -*-
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
import ast
import unittest
from copy import deepcopy

import httplib2
from googleapiclient.errors import HttpError

from airflow import AirflowException, configuration
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, \
    GceInstanceStopOperator, GceSetMachineTypeOperator, GceInstanceTemplateCopyOperator, \
    GceInstanceGroupManagerUpdateTemplateOperator
from airflow.models import TaskInstance, DAG
from airflow.utils import timezone

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

EMPTY_CONTENT = ''.encode('utf8')

PROJECT_ID = 'project-id'
ZONE = 'zone'
RESOURCE_ID = 'resource-id'
SHORT_MACHINE_TYPE_NAME = 'n1-machine-type'
SET_MACHINE_TYPE_BODY = {
    'machineType': 'zones/{}/machineTypes/{}'.format(ZONE, SHORT_MACHINE_TYPE_NAME)
}

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class GceInstanceStartTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_start(self, mock_hook):
        mock_hook.return_value.start_instance.return_value = True
        op = GceInstanceStartOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=RESOURCE_ID,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.start_instance.assert_called_once_with(
            PROJECT_ID, ZONE, RESOURCE_ID
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as template dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_start_with_templates(self, mock_hook):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GceInstanceStartOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'project_id'))
        self.assertEqual(dag_id, getattr(op, 'zone'))
        self.assertEqual(dag_id, getattr(op, 'resource_id'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_start_should_throw_ex_when_missing_project_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStartOperator(
                project_id="",
                zone=ZONE,
                resource_id=RESOURCE_ID,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'project_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_start_should_throw_ex_when_missing_zone(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStartOperator(
                project_id=PROJECT_ID,
                zone="",
                resource_id=RESOURCE_ID,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'zone' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_start_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStartOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'resource_id' is missing", str(err))
        mock_hook.assert_not_called()


class GceInstanceStopTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_stop(self, mock_hook):
        mock_hook.return_value.stop_instance.return_value = True
        op = GceInstanceStopOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=RESOURCE_ID,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.stop_instance.assert_called_once_with(
            PROJECT_ID, ZONE, RESOURCE_ID
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_stop_with_templates(self, mock_hook):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GceInstanceStopOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'project_id'))
        self.assertEqual(dag_id, getattr(op, 'zone'))
        self.assertEqual(dag_id, getattr(op, 'resource_id'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_stop_should_throw_ex_when_missing_project_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStopOperator(
                project_id="",
                zone=ZONE,
                resource_id=RESOURCE_ID,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'project_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_stop_should_throw_ex_when_missing_zone(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStopOperator(
                project_id=PROJECT_ID,
                zone="",
                resource_id=RESOURCE_ID,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'zone' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_stop_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceStopOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'resource_id' is missing", str(err))
        mock_hook.assert_not_called()


class GceInstanceSetMachineTypeTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type(self, mock_hook):
        mock_hook.return_value.set_machine_type.return_value = True
        op = GceSetMachineTypeOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            PROJECT_ID, ZONE, RESOURCE_ID, SET_MACHINE_TYPE_BODY
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type_with_templates(self, mock_hook):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GceSetMachineTypeOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            body={},
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'project_id'))
        self.assertEqual(dag_id, getattr(op, 'zone'))
        self.assertEqual(dag_id, getattr(op, 'resource_id'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type_should_throw_ex_when_missing_project_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id="",
                zone=ZONE,
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'project_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type_should_throw_ex_when_missing_zone(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id=PROJECT_ID,
                zone="",
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'zone' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id="",
                body=SET_MACHINE_TYPE_BODY,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'resource_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type_should_throw_ex_when_missing_machine_type(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id=RESOURCE_ID,
                body={},
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(
            "The required body field 'machineType' is missing. Please add it.", str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    MOCK_OP_RESPONSE = "{'kind': 'compute#operation', 'id': '8529919847974922736', " \
                       "'name': " \
                       "'operation-1538578207537-577542784f769-7999ab71-94f9ec1d', " \
                       "'zone': 'https://www.googleapis.com/compute/v1/projects/example" \
                       "-project/zones/europe-west3-b', 'operationType': " \
                       "'setMachineType', 'targetLink': " \
                       "'https://www.googleapis.com/compute/v1/projects/example-project" \
                       "/zones/europe-west3-b/instances/pa-1', 'targetId': " \
                       "'2480086944131075860', 'status': 'DONE', 'user': " \
                       "'service-account@example-project.iam.gserviceaccount.com', " \
                       "'progress': 100, 'insertTime': '2018-10-03T07:50:07.951-07:00', "\
                       "'startTime': '2018-10-03T07:50:08.324-07:00', 'endTime': " \
                       "'2018-10-03T07:50:08.484-07:00', 'error': {'errors': [{'code': " \
                       "'UNSUPPORTED_OPERATION', 'message': \"Machine type with name " \
                       "'machine-type-1' does not exist in zone 'europe-west3-b'.\"}]}, "\
                       "'httpErrorStatusCode': 400, 'httpErrorMessage': 'BAD REQUEST', " \
                       "'selfLink': " \
                       "'https://www.googleapis.com/compute/v1/projects/example-project" \
                       "/zones/europe-west3-b/operations/operation-1538578207537" \
                       "-577542784f769-7999ab71-94f9ec1d'} "

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook'
                '._check_zone_operation_status')
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook'
                '._execute_set_machine_type')
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook.get_conn')
    def test_set_machine_type_should_handle_and_trim_gce_error(
            self, get_conn, _execute_set_machine_type, _check_zone_operation_status):
        get_conn.return_value = {}
        _execute_set_machine_type.return_value = {"name": "test-operation"}
        _check_zone_operation_status.return_value = ast.literal_eval(self.MOCK_OP_RESPONSE)
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        _check_zone_operation_status.assert_called_once_with(
            {}, "test-operation", PROJECT_ID, ZONE)
        _execute_set_machine_type.assert_called_once_with(
            PROJECT_ID, ZONE, RESOURCE_ID, SET_MACHINE_TYPE_BODY)
        # Checking the full message was sometimes failing due to different order
        # of keys in the serialized JSON
        self.assertIn("400 BAD REQUEST: {", str(err))  # checking the square bracket trim
        self.assertIn("UNSUPPORTED_OPERATION", str(err))


GCE_INSTANCE_TEMPLATE_NAME = "instance-template-test"
GCE_INSTANCE_TEMPLATE_NEW_NAME = "instance-template-test-new"
GCE_INSTANCE_TEMPLATE_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"

GCE_INSTANCE_TEMPLATE_BODY_GET = {
    "kind": "compute#instanceTemplate",
    "id": "6950321349997439715",
    "creationTimestamp": "2018-10-15T06:20:12.777-07:00",
    "name": GCE_INSTANCE_TEMPLATE_NAME,
    "description": "",
    "properties": {
        "machineType": "n1-standard-1",
        "networkInterfaces": [
            {
                "kind": "compute#networkInterface",
                "network": "https://www.googleapis.com/compute/v1/"
                           "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "kind": "compute#accessConfig",
                        "type": "ONE_TO_ONE_NAT",
                    }
                ]
            },
            {
                "network": "https://www.googleapis.com/compute/v1/"
                           "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "kind": "compute#accessConfig",
                        "networkTier": "PREMIUM"
                    }
                ]
            }
        ],
        "disks": [
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "A String",
                ]
            }
        ],
        "metadata": {
            "kind": "compute#metadata",
            "fingerprint": "GDPUYxlwHe4="
        },
    },
    "selfLink": "https://www.googleapis.com/compute/v1/projects/project"
                "/global/instanceTemplates/instance-template-test"
}

GCE_INSTANCE_TEMPLATE_BODY_INSERT = {
    "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
    "description": "",
    "properties": {
        "machineType": "n1-standard-1",
        "networkInterfaces": [
            {
                "network": "https://www.googleapis.com/compute/v1/"
                           "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "type": "ONE_TO_ONE_NAT",
                    }
                ]
            },
            {
                "network": "https://www.googleapis.com/compute/v1/"
                           "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "networkTier": "PREMIUM"
                    }
                ]
            }
        ],
        "disks": [
            {
                "type": "PERSISTENT",
            }
        ],
        "metadata": {
            "fingerprint": "GDPUYxlwHe4="
        },
    },
}

GCE_INSTANCE_TEMPLATE_BODY_GET_NEW = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_GET)
GCE_INSTANCE_TEMPLATE_BODY_GET_NEW['name'] = GCE_INSTANCE_TEMPLATE_NEW_NAME


class GceInstanceTemplateCopyTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME}
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            request_id=None
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_idempotent_copy_template_when_already_copied(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME}
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.insert_instance_template.assert_not_called()
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template_with_request_id(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME}
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template_with_description_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                        "description": "New description"}
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["description"] = "New description"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=body_insert,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_copy_with_some_validation_warnings(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                        "some_wrong_field": "test",
                        "properties": {
                            "some_other_wrong_field": "test"
                        }}
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["some_wrong_field"] = "test"
        body_insert["properties"]["some_other_wrong_field"] = "test"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=body_insert,
            request_id=None,
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template_with_updated_nested_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "machineType": "n1-standard-2",
                }
            }
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["machineType"] = "n1-standard-2"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=body_insert,
            request_id=None
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template_with_smaller_array_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "machineType": "n1-standard-1",
                    "networkInterfaces": [
                        {
                            "network": "https://www.googleapis.com/compute/v1/"
                                       "projects/project/global/networks/default",
                            "accessConfigs": [
                                {
                                    "type": "ONE_TO_ONE_NAT",
                                    "natIP": "8.8.8.8"
                                }
                            ]
                        }
                    ]
                }
            }
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["networkInterfaces"] = [
            {
                "network": "https://www.googleapis.com/compute/v1/"
                           "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "type": "ONE_TO_ONE_NAT",
                        "natIP": "8.8.8.8"
                    }
                ]
            }
        ]
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=body_insert,
            request_id=None
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_copy_template_with_bigger_array_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        op = GceInstanceTemplateCopyOperator(
            project_id=PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "disks": [
                        {
                            "kind": "compute#attachedDisk",
                            "type": "SCRATCH",
                            "licenses": [
                                "Updated String",
                            ]
                        },
                        {
                            "kind": "compute#attachedDisk",
                            "type": "PERSISTENT",
                            "licenses": [
                                "Another String",
                            ]
                        }
                    ],
                }
            }
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["disks"] = [
            {
                "kind": "compute#attachedDisk",
                "type": "SCRATCH",
                "licenses": [
                    "Updated String",
                ]
            },
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "Another String",
                ]
            }
        ]
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=PROJECT_ID,
            body=body_insert,
            request_id=None,
        )
        self.assertEqual(GCE_INSTANCE_TEMPLATE_BODY_GET_NEW, result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_missing_name(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW
        ]
        with self.assertRaises(AirflowException) as cm:
            op = GceInstanceTemplateCopyOperator(
                project_id=PROJECT_ID,
                resource_id=GCE_INSTANCE_TEMPLATE_NAME,
                request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
                task_id='id',
                body_patch={"description": "New description"}
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("should contain at least name for the new operator "
                      "in the 'name' field", str(err))
        mock_hook.assert_not_called()


GCE_INSTANCE_GROUP_MANAGER_NAME = "instance-group-test"
GCE_INSTANCE_TEMPLATE_SOURCE_URL = \
    "https://www.googleapis.com/compute/beta/projects/project" \
    "/global/instanceTemplates/instance-template-test"

GCE_INSTANCE_TEMPLATE_OTHER_URL = \
    "https://www.googleapis.com/compute/beta/projects/project" \
    "/global/instanceTemplates/instance-template-other"

GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL = \
    "https://www.googleapis.com/compute/beta/projects/project" \
    "/global/instanceTemplates/instance-template-non-existing"

GCE_INSTANCE_TEMPLATE_DESTINATION_URL = \
    "https://www.googleapis.com/compute/beta/projects/project" \
    "/global/instanceTemplates/instance-template-new"

GCE_INSTANCE_GROUP_MANAGER_GET = {
    "kind": "compute#instanceGroupManager",
    "id": "2822359583810032488",
    "creationTimestamp": "2018-10-17T05:39:35.793-07:00",
    "name": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "zone": "https://www.googleapis.com/compute/beta/projects/project/zones/zone",
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "versions": [
        {
            "name": "v1",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            "targetSize": {
                "calculated": 1
            }
        },
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        }
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
        "refreshing": 0
    },
    "pendingActions": {
        "creating": 0,
        "deleting": 0,
        "recreating": 0,
        "restarting": 0
    },
    "targetSize": 1,
    "selfLink": "https://www.googleapis.com/compute/beta/projects/project/zones/"
                "zone/instanceGroupManagers/" + GCE_INSTANCE_GROUP_MANAGER_NAME,
    "autoHealingPolicies": [
        {
            "initialDelaySec": 300
        }
    ],
    "serviceAccount": "198907790164@cloudservices.gserviceaccount.com"
}

GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH = {
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
    "versions": [
        {
            "name": "v1",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            "targetSize": {
                "calculated": 1
            }
        },
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        }
    ],
}

GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"

GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {
        "fixed": 1
    },
    "maxUnavailable": {
        "percent": 10
    },
    "minReadySec": 1800
}


class GceInstanceGroupManagerUpdateTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_instance_group_update(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=None
        )
        self.assertTrue(result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_instance_group_update_no_instance_template_field(self, mock_hook):
        instance_group_manager_no_template = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_template['instanceTemplate']
        mock_hook.return_value.get_instance_group_manager.return_value = \
            instance_group_manager_no_template
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        expected_patch_no_instance_template = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_instance_template['instanceTemplate']
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_instance_template,
            request_id=None
        )
        self.assertTrue(result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_instance_group_update_no_versions_field(self, mock_hook):
        instance_group_manager_no_versions = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_versions['versions']
        mock_hook.return_value.get_instance_group_manager.return_value = \
            instance_group_manager_no_versions
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        expected_patch_no_versions = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_versions['versions']
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_versions,
            request_id=None
        )
        self.assertTrue(result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_instance_group_update_with_update_policy(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        expected_patch_with_update_policy = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        expected_patch_with_update_policy['updatePolicy'] = \
            GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_with_update_policy,
            request_id=None
        )
        self.assertTrue(result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_successful_instance_group_update_with_request_id(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID
        )
        self.assertTrue(result)

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_try_to_use_api_v1(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GceInstanceGroupManagerUpdateTemplateOperator(
                project_id=PROJECT_ID,
                zone=ZONE,
                resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
                task_id='id',
                api_version='v1',
                source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
                destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
            )
        err = cm.exception
        self.assertIn("Use beta api version or above", str(err))

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_try_to_use_non_existing_template(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = \
            deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        op = GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            zone=ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='beta',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.patch_instance_group_manager.assert_not_called()
        self.assertTrue(result)
