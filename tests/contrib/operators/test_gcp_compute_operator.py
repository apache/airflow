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

from airflow import AirflowException, configuration
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, \
    GceInstanceStopOperator, GceSetMachineTypeOperator
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

PROJECT_ID = 'project-id'
LOCATION = 'zone'
RESOURCE_ID = 'resource-id'
SHORT_MACHINE_TYPE_NAME = 'n1-machine-type'
SET_MACHINE_TYPE_BODY = {
    'machineType': 'zones/{}/machineTypes/{}'.format(LOCATION, SHORT_MACHINE_TYPE_NAME)
}

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class GceInstanceStartTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_start(self, mock_hook):
        mock_hook.return_value.start_instance.return_value = True
        op = GceInstanceStartOperator(
            project_id=PROJECT_ID,
            zone=LOCATION,
            resource_id=RESOURCE_ID,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.start_instance.assert_called_once_with(
            PROJECT_ID, LOCATION, RESOURCE_ID
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
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
                zone=LOCATION,
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
                zone=LOCATION,
                resource_id="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'resource_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_instance_stop(self, mock_hook):
        mock_hook.return_value.stop_instance.return_value = True
        op = GceInstanceStopOperator(
            project_id=PROJECT_ID,
            zone=LOCATION,
            resource_id=RESOURCE_ID,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.stop_instance.assert_called_once_with(
            PROJECT_ID, LOCATION, RESOURCE_ID
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
                zone=LOCATION,
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
                zone=LOCATION,
                resource_id="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'resource_id' is missing", str(err))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook')
    def test_set_machine_type(self, mock_hook):
        mock_hook.return_value.set_machine_type.return_value = True
        op = GceSetMachineTypeOperator(
            project_id=PROJECT_ID,
            zone=LOCATION,
            resource_id=RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            PROJECT_ID, LOCATION, RESOURCE_ID, SET_MACHINE_TYPE_BODY
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
                zone=LOCATION,
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
                zone=LOCATION,
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
                zone=LOCATION,
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
                       "'zone': 'https://www.googleapis.com/compute/v1/projects/polidea" \
                       "-airflow/zones/europe-west3-b', 'operationType': " \
                       "'setMachineType', 'targetLink': " \
                       "'https://www.googleapis.com/compute/v1/projects/polidea-airflow" \
                       "/zones/europe-west3-b/instances/pa-1', 'targetId': " \
                       "'2480086944131075860', 'status': 'DONE', 'user': " \
                       "'uberdarek@polidea-airflow.iam.gserviceaccount.com', " \
                       "'progress': 100, 'insertTime': '2018-10-03T07:50:07.951-07:00', "\
                       "'startTime': '2018-10-03T07:50:08.324-07:00', 'endTime': " \
                       "'2018-10-03T07:50:08.484-07:00', 'error': {'errors': [{'code': " \
                       "'UNSUPPORTED_OPERATION', 'message': \"Machine type with name " \
                       "'machine-type-1' does not exist in zone 'europe-west3-b'.\"}]}, "\
                       "'httpErrorStatusCode': 400, 'httpErrorMessage': 'BAD REQUEST', " \
                       "'selfLink': " \
                       "'https://www.googleapis.com/compute/v1/projects/polidea-airflow" \
                       "/zones/europe-west3-b/operations/operation-1538578207537" \
                       "-577542784f769-7999ab71-94f9ec1d'} "

    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook'
                '._check_operation_status')
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook'
                '._execute_set_machine_type')
    @mock.patch('airflow.contrib.operators.gcp_compute_operator.GceHook.get_conn')
    def test_set_machine_type_should_handle_and_trim_gce_error(
            self, get_conn, _execute_set_machine_type, _check_operation_status):
        get_conn.return_value = {}
        _execute_set_machine_type.return_value = {"name": "test-operation"}
        _check_operation_status.return_value = ast.literal_eval(self.MOCK_OP_RESPONSE)
        with self.assertRaises(AirflowException) as cm:
            op = GceSetMachineTypeOperator(
                project_id=PROJECT_ID,
                zone=LOCATION,
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        _check_operation_status.assert_called_once_with(
            {}, "test-operation", PROJECT_ID, LOCATION)
        _execute_set_machine_type.assert_called_once_with(
            PROJECT_ID, LOCATION, RESOURCE_ID, SET_MACHINE_TYPE_BODY)
        # Checking the full message was sometimes failing due to different order
        # of keys in the serialized JSON
        self.assertIn("400 BAD REQUEST: {", str(err))  # checking the square bracket trim
        self.assertIn("UNSUPPORTED_OPERATION", str(err))
