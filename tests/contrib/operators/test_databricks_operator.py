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
#

import unittest
from datetime import datetime

import mock

import airflow.contrib.operators.databricks_operator as databricks_operator
from airflow.contrib.hooks.databricks_hook import RunState
from airflow.contrib.operators.databricks_operator import (
    DatabricksRunNowOperator, DatabricksSubmitRunOperator,
)
from airflow.exceptions import AirflowException
from airflow.models import DAG

DATE = '2017-04-20'
TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {
    'notebook_path': '/test'
}
TEMPLATED_NOTEBOOK_TASK = {
    'notebook_path': '/test-{{ ds }}'
}
RENDERED_TEMPLATED_NOTEBOOK_TASK = {
    'notebook_path': '/test-{0}'.format(DATE)
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
JOB_ID = 42
NOTEBOOK_PARAMS = {
    "dry-run": "true",
    "oldest-time-to-consider": "1457570074236"
}
JAR_PARAMS = ["param1", "param2"]
RENDERED_TEMPLATED_JAR_PARAMS = [
    '/test-{0}'.format(DATE)
]
TEMPLATED_JAR_PARAMS = [
    '/test-{{ ds }}'
]
PYTHON_PARAMS = ["john doe", "35"]
SPARK_SUBMIT_PARAMS = ["--class", "org.apache.spark.examples.SparkPi"]


class TestDatabricksOperatorSharedFunctions(unittest.TestCase):
    def test_deep_string_coerce(self):
        test_json = {
            'test_int': 1,
            'test_float': 1.0,
            'test_dict': {'key': 'value'},
            'test_list': [1, 1.0, 'a', 'b'],
            'test_tuple': (1, 1.0, 'a', 'b')
        }

        expected = {
            'test_int': '1',
            'test_float': '1.0',
            'test_dict': {'key': 'value'},
            'test_list': ['1', '1.0', 'a', 'b'],
            'test_tuple': ['1', '1.0', 'a', 'b']
        }
        self.assertDictEqual(databricks_operator._deep_string_coerce(test_json), expected)


class TestDatabricksSubmitRunOperator(unittest.TestCase):
    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID,
                                         new_cluster=NEW_CLUSTER,
                                         notebook_task=NOTEBOOK_TASK)
        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': TASK_ID
        })

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
        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': TASK_ID
        })
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
        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': RUN_NAME
        })
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
        op = DatabricksSubmitRunOperator(task_id=TASK_ID,
                                         json=json,
                                         new_cluster=override_new_cluster)
        expected = databricks_operator._deep_string_coerce({
            'new_cluster': override_new_cluster,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': TASK_ID,
        })
        self.assertDictEqual(expected, op.json)

    def test_init_with_templating(self):
        json = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': TEMPLATED_NOTEBOOK_TASK,
        }
        dag = DAG('test', start_date=datetime.now())
        op = DatabricksSubmitRunOperator(dag=dag, task_id=TASK_ID, json=json)
        op.render_template_fields(context={'ds': DATE})
        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': RENDERED_TEMPLATED_NOTEBOOK_TASK,
            'run_name': TASK_ID,
        })
        self.assertDictEqual(expected, op.json)

    def test_init_with_bad_type(self):
        json = {
            'test': datetime.now()
        }
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = r'Type \<(type|class) \'datetime.datetime\'\> used ' + \
                            r'for parameter json\[test\] is not a number or a string'
        with self.assertRaisesRegex(AirflowException, exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)

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

        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': TASK_ID
        })
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay)

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEqual(RUN_ID, op.run_id)

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

        expected = databricks_operator._deep_string_coerce({
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
            'run_name': TASK_ID,
        })
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay)
        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEqual(RUN_ID, op.run_id)

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


class TestDatabricksRunNowOperator(unittest.TestCase):

    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksRunNowOperator(job_id=JOB_ID, task_id=TASK_ID)
        expected = databricks_operator._deep_string_coerce({
            'job_id': 42
        })

        self.assertDictEqual(expected, op.json)

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': JAR_PARAMS,
            'python_params': PYTHON_PARAMS,
            'spark_submit_params': SPARK_SUBMIT_PARAMS,
            'job_id': JOB_ID
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, json=json)

        expected = databricks_operator._deep_string_coerce({
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': JAR_PARAMS,
            'python_params': PYTHON_PARAMS,
            'spark_submit_params': SPARK_SUBMIT_PARAMS,
            'job_id': JOB_ID
        })

        self.assertDictEqual(expected, op.json)

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_notebook_params = {'workers': 999}
        json = {
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': JAR_PARAMS
        }

        op = DatabricksRunNowOperator(task_id=TASK_ID,
                                      json=json,
                                      job_id=JOB_ID,
                                      notebook_params=override_notebook_params,
                                      python_params=PYTHON_PARAMS,
                                      spark_submit_params=SPARK_SUBMIT_PARAMS)

        expected = databricks_operator._deep_string_coerce({
            'notebook_params': override_notebook_params,
            'jar_params': JAR_PARAMS,
            'python_params': PYTHON_PARAMS,
            'spark_submit_params': SPARK_SUBMIT_PARAMS,
            'job_id': JOB_ID
        })

        self.assertDictEqual(expected, op.json)

    def test_init_with_templating(self):
        json = {
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': TEMPLATED_JAR_PARAMS
        }

        dag = DAG('test', start_date=datetime.now())
        op = DatabricksRunNowOperator(dag=dag, task_id=TASK_ID, job_id=JOB_ID, json=json)
        op.render_template_fields(context={'ds': DATE})
        expected = databricks_operator._deep_string_coerce({
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': RENDERED_TEMPLATED_JAR_PARAMS,
            'job_id': JOB_ID
        })
        self.assertDictEqual(expected, op.json)

    def test_init_with_bad_type(self):
        json = {
            'test': datetime.now()
        }
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = r'Type \<(type|class) \'datetime.datetime\'\> used ' + \
                            r'for parameter json\[test\] is not a number or a string'
        with self.assertRaisesRegex(AirflowException, exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
            'notebook_params': NOTEBOOK_PARAMS,
            'notebook_task': NOTEBOOK_TASK,
            'jar_params': JAR_PARAMS
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        expected = databricks_operator._deep_string_coerce({
            'notebook_params': NOTEBOOK_PARAMS,
            'notebook_task': NOTEBOOK_TASK,
            'jar_params': JAR_PARAMS,
            'job_id': JOB_ID
        })

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay)
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEqual(RUN_ID, op.run_id)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {
            'notebook_params': NOTEBOOK_PARAMS,
            'notebook_task': NOTEBOOK_TASK,
            'jar_params': JAR_PARAMS
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'FAILED', '')

        with self.assertRaises(AirflowException):
            op.execute(None)

        expected = databricks_operator._deep_string_coerce({
            'notebook_params': NOTEBOOK_PARAMS,
            'notebook_task': NOTEBOOK_TASK,
            'jar_params': JAR_PARAMS,
            'job_id': JOB_ID
        })
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay)
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        self.assertEqual(RUN_ID, op.run_id)

    @mock.patch('airflow.contrib.operators.databricks_operator.DatabricksHook')
    def test_on_kill(self, db_mock_class):
        run = {
            'notebook_params': NOTEBOOK_PARAMS,
            'notebook_task': NOTEBOOK_TASK,
            'jar_params': JAR_PARAMS
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()
        db_mock.cancel_run.assert_called_once_with(RUN_ID)
