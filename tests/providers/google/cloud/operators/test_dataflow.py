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

from unittest import mock

from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
    DataflowStartFlexTemplateOperator,
)
from airflow.version import version

TASK_ID = 'test-dataflow-operator'
JOB_ID = 'test-dataflow-pipeline-id'
JOB_NAME = 'test-dataflow-pipeline-name'
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output',
}
PY_FILE = 'gs://my-bucket/my-object.py'
PY_INTERPRETER = 'python3'
JAR_FILE = 'gs://my-bucket/example/test.jar'
JOB_CLASS = 'com.test.NotMain'
PY_OPTIONS = ['-m']
DEFAULT_OPTIONS_PYTHON = DEFAULT_OPTIONS_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
}
DEFAULT_OPTIONS_TEMPLATE = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
    'tempLocation': 'gs://test/temp',
    'zone': 'us-central1-f',
}
ADDITIONAL_OPTIONS = {'output': 'gs://test/output', 'labels': {'foo': 'bar'}}
TEST_VERSION = 'v{}'.format(version.replace('.', '-').replace('+', '-'))
EXPECTED_ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
}
POLL_SLEEP = 30
GCS_HOOK_STRING = 'airflow.providers.google.cloud.operators.dataflow.{}'
TEST_FLEX_PARAMETERS = {
    "containerSpecGcsPath": "gs://test-bucket/test-file",
    "jobName": 'test-job-name',
    "parameters": {
        "inputSubscription": 'test-subsription',
        "outputTable": "test-project:test-dataset.streaming_beam_sql",
    },
}
TEST_LOCATION = 'custom-location'
TEST_PROJECT_ID = 'test-project-id'


class TestDataflowPythonOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreatePythonJobOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            job_name=JOB_NAME,
            py_options=PY_OPTIONS,
            dataflow_default_options=DEFAULT_OPTIONS_PYTHON,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.job_name, JOB_NAME)
        self.assertEqual(self.dataflow.py_file, PY_FILE)
        self.assertEqual(self.dataflow.py_options, PY_OPTIONS)
        self.assertEqual(self.dataflow.py_interpreter, PY_INTERPRETER)
        self.assertEqual(self.dataflow.poll_sleep, POLL_SLEEP)
        self.assertEqual(self.dataflow.dataflow_default_options, DEFAULT_OPTIONS_PYTHON)
        self.assertEqual(self.dataflow.options, EXPECTED_ADDITIONAL_OPTIONS)

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_exec(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_python_workflow.

        """
        start_python_hook = dataflow_mock.return_value.start_python_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        expected_options = {
            'project': 'test',
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
        }
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        start_python_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=expected_options,
            dataflow=mock.ANY,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
        )
        self.assertTrue(self.dataflow.py_file.startswith('/tmp/dataflow'))


class TestDataflowJavaOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreateJavaJobOperator(
            task_id=TASK_ID,
            jar=JAR_FILE,
            job_name=JOB_NAME,
            job_class=JOB_CLASS,
            dataflow_default_options=DEFAULT_OPTIONS_JAVA,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.job_name, JOB_NAME)
        self.assertEqual(self.dataflow.poll_sleep, POLL_SLEEP)
        self.assertEqual(self.dataflow.dataflow_default_options, DEFAULT_OPTIONS_JAVA)
        self.assertEqual(self.dataflow.job_class, JOB_CLASS)
        self.assertEqual(self.dataflow.jar, JAR_FILE)
        self.assertEqual(self.dataflow.options, EXPECTED_ADDITIONAL_OPTIONS)
        self.assertEqual(self.dataflow.check_if_running, CheckJobRunning.WaitForRun)

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_exec(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = CheckJobRunning.IgnoreJob
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=mock.ANY,
            jar=mock.ANY,
            job_class=JOB_CLASS,
            append_job_name=True,
            multiple_jobs=None,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_job_running_exec(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        dataflow_running = dataflow_mock.return_value.is_job_dataflow_running
        dataflow_running.return_value = True
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_provide_file.assert_not_called()
        start_java_hook.assert_not_called()
        dataflow_running.assert_called_once_with(
            name=JOB_NAME, variables=mock.ANY, project_id=None, location=TEST_LOCATION
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_job_not_running_exec(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running

        """
        dataflow_running = dataflow_mock.return_value.is_job_dataflow_running
        dataflow_running.return_value = False
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=mock.ANY,
            jar=mock.ANY,
            job_class=JOB_CLASS,
            append_job_name=True,
            multiple_jobs=None,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
        )
        dataflow_running.assert_called_once_with(
            name=JOB_NAME, variables=mock.ANY, project_id=None, location=TEST_LOCATION
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_multiple_job_exec(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check multiple jobs

        """
        dataflow_running = dataflow_mock.return_value.is_job_dataflow_running
        dataflow_running.return_value = False
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.multiple_jobs = True
        self.dataflow.check_if_running = True
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=mock.ANY,
            jar=mock.ANY,
            job_class=JOB_CLASS,
            append_job_name=True,
            multiple_jobs=True,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
        )
        dataflow_running.assert_called_once_with(
            name=JOB_NAME, variables=mock.ANY, project_id=None, location=TEST_LOCATION
        )


class TestDataflowTemplateOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowTemplatedJobStartOperator(
            task_id=TASK_ID,
            template=TEMPLATE,
            job_name=JOB_NAME,
            parameters=PARAMETERS,
            options=DEFAULT_OPTIONS_TEMPLATE,
            dataflow_default_options={"EXTRA_OPTION": "TEST_A"},
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    def test_exec(self, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_template_workflow.

        """
        start_template_hook = dataflow_mock.return_value.start_template_dataflow
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        expected_options = {
            'project': 'test',
            'stagingLocation': 'gs://test/staging',
            'tempLocation': 'gs://test/temp',
            'zone': 'us-central1-f',
            'EXTRA_OPTION': "TEST_A",
        }
        start_template_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=expected_options,
            parameters=PARAMETERS,
            dataflow_template=TEMPLATE,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
            environment={'maxWorkers': 2},
        )


class TestDataflowStartFlexTemplateOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    def test_execute(self, mock_dataflow):
        start_flex_template = DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        start_flex_template.execute(mock.MagicMock())
        mock_dataflow.return_value.start_flex_template.assert_called_once_with(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            on_new_job_id_callback=mock.ANY,
        )

    def test_on_kill(self):
        start_flex_template = DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
        )
        start_flex_template.hook = mock.MagicMock()
        start_flex_template.job_id = JOB_ID
        start_flex_template.on_kill()
        start_flex_template.hook.cancel_job.assert_called_once_with(
            job_id='test-dataflow-pipeline-id', project_id=TEST_PROJECT_ID
        )
