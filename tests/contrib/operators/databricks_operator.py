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
    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, new_cluster=NEW_CLUSTER, notebook_task=NOTEBOOK_TASK)
        expected = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': TASK_ID
        }
        self.assertDictEqual(expected, op.json)

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': TASK_ID
        }
        self.assertDictEqual(expected, op.json)

    def test_init_with_specified_run_name(self):
        """
        Test the initializer with a specified run_name.
        """
        json = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': RUN_NAME
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': RUN_NAME
        }
        self.assertDictEqual(expected, op.json)

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_new_cluster = {'workers': 999}
        json = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json, new_cluster=override_new_cluster)
        expected = {
          'new_cluster': override_new_cluster,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': TASK_ID,
        }
        self.assertDictEqual(expected, op.json)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        expected = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': TASK_ID
        }
        db_mock_class.assert_called_once_with(
                DEFAULT_CONN_ID,
                retry_limit=op.databricks_retry_limit)
        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEquals(RUN_ID, op.run_id)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'FAILED', '')

        with self.assertRaises(AirflowException):
            op.execute(None)

        expected = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
          'run_name': TASK_ID,
        }
        db_mock_class.assert_called_once_with(
                DEFAULT_CONN_ID,
                retry_limit=op.databricks_retry_limit)
        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEquals(RUN_ID, op.run_id)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_on_kill(self, db_mock_class):
        run = {
          'new_cluster': NEW_CLUSTER,
          'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()

        db_mock.cancel_run.assert_called_once_with(RUN_ID)

