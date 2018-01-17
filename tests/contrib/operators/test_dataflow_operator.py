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

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator, \
    DataFlowJavaOperator, DataflowTemplateOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.version import version

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


TASK_ID = 'test-dataflow-operator'
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output'
}
PY_FILE = 'gs://my-bucket/my-object.py'
JAR_FILE = 'example/test.jar'
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
    'zone': 'us-central1-f'
}
ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar'}
}
TEST_VERSION = 'v{}'.format(version.replace('.', '-').replace('+', '-'))
EXPECTED_ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION}
}
POLL_SLEEP = 30
GCS_HOOK_STRING = 'airflow.contrib.operators.dataflow_operator.{}'


class DataFlowPythonOperatorTest(unittest.TestCase):

    def setUp(self):
        self.dataflow = DataFlowPythonOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            dataflow_default_options=DEFAULT_OPTIONS_PYTHON,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP)

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.py_file, PY_FILE)
        self.assertEqual(self.dataflow.py_options, PY_OPTIONS)
        self.assertEqual(self.dataflow.poll_sleep, POLL_SLEEP)
        self.assertEqual(self.dataflow.dataflow_default_options,
                         DEFAULT_OPTIONS_PYTHON)
        self.assertEqual(self.dataflow.options,
                         EXPECTED_ADDITIONAL_OPTIONS)

    @mock.patch('airflow.contrib.operators.dataflow_operator.DataFlowHook')
    @mock.patch(GCS_HOOK_STRING.format('GoogleCloudBucketHelper'))
    def test_exec(self, gcs_hook, dataflow_mock):
        """Test DataFlowHook is created and the right args are passed to
        start_python_workflow.

        """
        start_python_hook = dataflow_mock.return_value.start_python_dataflow
        gcs_download_hook = gcs_hook.return_value.google_cloud_to_local
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        expected_options = {
            'project': 'test',
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION}
        }
        gcs_download_hook.assert_called_once_with(PY_FILE)
        start_python_hook.assert_called_once_with(TASK_ID, expected_options,
                                                  mock.ANY, PY_OPTIONS)
        self.assertTrue(self.dataflow.py_file.startswith('/tmp/dataflow'))


class DataFlowJavaOperatorTest(unittest.TestCase):

    def setUp(self):
        self.dataflow = DataFlowJavaOperator(
            task_id=TASK_ID,
            jar=JAR_FILE,
            job_class=JOB_CLASS,
            dataflow_default_options=DEFAULT_OPTIONS_JAVA,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP)

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.poll_sleep, POLL_SLEEP)
        self.assertEqual(self.dataflow.dataflow_default_options,
                         DEFAULT_OPTIONS_JAVA)
        self.assertEqual(self.dataflow.job_class, JOB_CLASS)
        self.assertEqual(self.dataflow.jar, JAR_FILE)
        self.assertEqual(self.dataflow.options,
                         EXPECTED_ADDITIONAL_OPTIONS)

    @mock.patch('airflow.contrib.operators.dataflow_operator.DataFlowHook')
    @mock.patch(GCS_HOOK_STRING.format('GoogleCloudBucketHelper'))
    def test_exec(self, gcs_hook, dataflow_mock):
        """Test DataFlowHook is created and the right args are passed to
        start_java_workflow.

        """
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        gcs_download_hook = gcs_hook.return_value.google_cloud_to_local
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_download_hook.assert_called_once_with(JAR_FILE)
        start_java_hook.assert_called_once_with(TASK_ID, mock.ANY,
                                                mock.ANY, JOB_CLASS)


class DataFlowTemplateOperatorTest(unittest.TestCase):

    def setUp(self):
        self.dataflow = DataflowTemplateOperator(
            task_id=TASK_ID,
            template=TEMPLATE,
            parameters=PARAMETERS,
            dataflow_default_options=DEFAULT_OPTIONS_TEMPLATE,
            poll_sleep=POLL_SLEEP)

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        self.assertEqual(self.dataflow.task_id, TASK_ID)
        self.assertEqual(self.dataflow.template, TEMPLATE)
        self.assertEqual(self.dataflow.parameters, PARAMETERS)
        self.assertEqual(self.dataflow.poll_sleep, POLL_SLEEP)
        self.assertEqual(self.dataflow.dataflow_default_options,
                         DEFAULT_OPTIONS_TEMPLATE)

    @mock.patch('airflow.contrib.operators.dataflow_operator.DataFlowHook')
    def test_exec(self, dataflow_mock):
        """Test DataFlowHook is created and the right args are passed to
        start_template_workflow.

        """
        start_template_hook = dataflow_mock.return_value.start_template_dataflow
        self.dataflow.execute(None)
        self.assertTrue(dataflow_mock.called)
        expected_options = {
            'project': 'test',
            'stagingLocation': 'gs://test/staging',
            'tempLocation': 'gs://test/temp',
            'zone': 'us-central1-f'
        }
        start_template_hook.assert_called_once_with(TASK_ID, expected_options,
                                                    PARAMETERS, TEMPLATE)
