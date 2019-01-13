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
import unittest

from airflow import AirflowException, configuration
from airflow.contrib.operators.gcp_transfer_operator import GcpStorageTransferOperationsCancelOperator, \
    GcpStorageTransferOperationsResumeOperator
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

GCP_PROJECT_ID = 'project-id'

OPERATION_NAME = "operation-name"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class GcpStorageTransferJobCreateOperatorTest(unittest.TestCase):
    pass


class GpcStorageTransferJobUpdateOperatorTest(unittest.TestCase):
    pass


class GpcStorageTransferOperationsGetOperatorTest(unittest.TestCase):
    pass


class GcpStorageTransferOperationsListOperatorTest(unittest.TestCase):
    pass


class GcpStorageTransferOperationsPauseOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause(self, mock_hook):
        mock_hook.return_value.stop_instance.return_value = True
        op = GcpStorageTransferOperationsCancelOperator(
            operation_name=OPERATION_NAME,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.cancel_transfer_operation.assert_called_once_with(
            OPERATION_NAME
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpStorageTransferOperationsCancelOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpStorageTransferOperationsCancelOperator(
                operation_name="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsResumeOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume(self, mock_hook):
        mock_hook.return_value.stop_instance.return_value = True
        op = GcpStorageTransferOperationsResumeOperator(
            operation_name=OPERATION_NAME,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.resume_transfer_operation.assert_called_once_with(
            OPERATION_NAME
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpStorageTransferOperationsResumeOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpStorageTransferOperationsResumeOperator(
                operation_name="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsCancelOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel(self, mock_hook):
        mock_hook.return_value.stop_instance.return_value = True
        op = GcpStorageTransferOperationsCancelOperator(
            operation_name=OPERATION_NAME,
            task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.cancel_transfer_operation.assert_called_once_with(
            OPERATION_NAME
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpStorageTransferOperationsCancelOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))


    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpStorageTransferOperationsCancelOperator(
                operation_name="",
                task_id='id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()

