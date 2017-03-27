# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest

from airflow.contrib.hooks.databricks_hook import RunState
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.exceptions import AirflowException

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {
    'notebook_path': '/test'
}
SPARK_JAR_TASK = {
    'main_class_name': 'com.databricks.Test'
}
NEW_CLUSTER = {
    'spark_version': '2.0.x-scala2.10',
    'node_type_id': 'development-node',
    'num_workers': 1
}
EXISTING_CLUSTER_ID = 'existing-cluster-id'
RUN_NAME = 'run-name'
RUN_ID = 1


class DatabricksSubmitRunOperatorTest(unittest.TestCase):

    def _test_init(
            self,
            op,
            expected_spark_jar_task,
            expected_notebook_task,
            expected_existing_cluster_id,
            expected_libraries,
            expected_run_name,
            expected_timeout_seconds,
            expected_extra_api_parameters,
            expected_databricks_conn_id):
        """
        Utility method to test behavior of initializer.
        """
        self.assertEqual(op.spark_jar_task, expected_spark_jar_task)
        self.assertEqual(op.notebook_task, expected_notebook_task)
        self.assertEqual(op.existing_cluster_id, expected_existing_cluster_id)
        self.assertEqual(op.libraries, expected_libraries)
        self.assertEqual(op.run_name, expected_run_name)
        self.assertEqual(op.timeout_seconds, expected_timeout_seconds)
        self.assertEqual(op.extra_api_parameters, expected_extra_api_parameters)
        self.assertEqual(op.databricks_conn_id, expected_databricks_conn_id)

    def test_init_with_unpacked_json(self):
        """
        Test the initializer with a unpacked json data.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, **run)
        self._test_init(
            op,
            expected_spark_jar_task=None,
            expected_notebook_task=NOTEBOOK_TASK,
            expected_existing_cluster_id=None,
            expected_libraries=None,
            expected_run_name=TASK_ID,
            expected_timeout_seconds=0,
            expected_extra_api_parameters={},
            expected_databricks_conn_id=DEFAULT_CONN_ID)

    def test_init_with_unpacked_json_and_run_name(self):
        """
        Test the initializer with a unpacked json data and specified run_name.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': RUN_NAME
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, **run)
        self._test_init(
            op,
            expected_spark_jar_task=None,
            expected_notebook_task=NOTEBOOK_TASK,
            expected_existing_cluster_id=None,
            expected_libraries=None,
            expected_run_name=RUN_NAME,
            expected_timeout_seconds=0,
            expected_extra_api_parameters={},
            expected_databricks_conn_id=DEFAULT_CONN_ID)

    def test_init_with_named_parameters(self):
        """
        Test the initializer which uses the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID,
            new_cluster=NEW_CLUSTER,
            notebook_task=NOTEBOOK_TASK)
        self._test_init(
            op,
            expected_spark_jar_task=None,
            expected_notebook_task=NOTEBOOK_TASK,
            expected_existing_cluster_id=None,
            expected_libraries=None,
            expected_run_name=TASK_ID,
            expected_timeout_seconds=0,
            expected_extra_api_parameters={},
            expected_databricks_conn_id=DEFAULT_CONN_ID)

    def test_init_with_positional_parameters(self):
        """
        Test the initializer which uses positional parameters. Users should not
        use it this way but we cannot enforce this.
        """
        op = DatabricksSubmitRunOperator(
            None,
            NOTEBOOK_TASK,
            NEW_CLUSTER,
            None,
            [],
            None,
            0,
            {},
            'databricks_default',
            task_id=TASK_ID)
        self._test_init(
            op,
            expected_spark_jar_task=None,
            expected_notebook_task=NOTEBOOK_TASK,
            expected_existing_cluster_id=None,
            expected_libraries=[],
            expected_run_name=TASK_ID,
            expected_timeout_seconds=0,
            expected_extra_api_parameters={},
            expected_databricks_conn_id=DEFAULT_CONN_ID)

    def test_init_with_invalid_oneof_task(self):
        """
        Test the initializer where notebook_task and spark_jar_task are
        both specified. This is not allowed and an exception should
        be raised.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'spark_jar_task': SPARK_JAR_TASK
        }
        with self.assertRaises(AirflowException):
            DatabricksSubmitRunOperator(task_id=TASK_ID, **run)

    def test_init_with_invalid_oneof_cluster(self):
        """
        Test the initializer where notebook_task and spark_jar_task are
        both specified. This is not allowed and an exception should
        be raised.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'existing_cluster_id': EXISTING_CLUSTER_ID
        }
        with self.assertRaises(AirflowException):
            DatabricksSubmitRunOperator(task_id=TASK_ID, **run)

    def test_init_with_invalid_oneof_task(self):
        """
        Test the initializer where notebook_task and spark_jar_task are
        both NOT specified. This is not allowed and an exception should
        be raised.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
        }
        with self.assertRaises(AirflowException):
            DatabricksSubmitRunOperator(task_id=TASK_ID, **run)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, **run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        db_mock_class.assert_called_once_with(DEFAULT_CONN_ID)
        db_mock.submit_run.assert_called_once_with(
            None,           # spark_jar_task
            NOTEBOOK_TASK,  # notebook_task
            NEW_CLUSTER,    # new_cluster
            None,           # existing_cluster_id
            None,           # libraries
            TASK_ID,        # run_name
            0)              # timeout_seconds
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, **run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        db_mock_class.assert_called_once_with(DEFAULT_CONN_ID)
        db_mock.submit_run.assert_called_once_with(
            None,           # spark_jar_task
            NOTEBOOK_TASK,  # notebook_task
            NEW_CLUSTER,    # new_cluster
            None,           # existing_cluster_id
            None,           # libraries
            TASK_ID,        # run_name
            0)              # timeout_seconds
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_extra_param(self, db_mock_class):
        """
        Test the execute function in case where extra API parameters are used.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'extra_api_parameters': {'test_param': '1'},
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, **run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'FAILED', '')

        with self.assertRaises(AirflowException):
            op.execute(None)

        db_mock_class.assert_called_once_with(DEFAULT_CONN_ID)
        db_mock.submit_run.assert_called_once_with(
            None,           # spark_jar_task
            NOTEBOOK_TASK,  # notebook_task
            NEW_CLUSTER,    # new_cluster
            None,           # existing_cluster_id
            None,           # libraries
            TASK_ID,        # run_name
            0,              # timeout_seconds
            test_param='1')
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
