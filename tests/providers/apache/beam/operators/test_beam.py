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

from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)
from airflow.version import version

TASK_ID = 'test-beam-operator'
DEFAULT_RUNNER = "DirectRunner"
JOB_NAME = 'test-dataflow-pipeline-name'
JOB_ID = 'test-dataflow-pipeline-id'
JAR_FILE = 'gs://my-bucket/example/test.jar'
JOB_CLASS = 'com.test.NotMain'
PY_FILE = 'gs://my-bucket/my-object.py'
PY_INTERPRETER = 'python3'
PY_OPTIONS = ['-m']
DEFAULT_OPTIONS_PYTHON = DEFAULT_OPTIONS_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
}
ADDITIONAL_OPTIONS = {'output': 'gs://test/output', 'labels': {'foo': 'bar'}}
TEST_VERSION = 'v{}'.format(version.replace('.', '-').replace('+', '-'))
EXPECTED_ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
}


class TestBeamRunPythonPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.operator = BeamRunPythonPipelineOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            default_pipeline_options=DEFAULT_OPTIONS_PYTHON,
            pipeline_options=ADDITIONAL_OPTIONS,
            job_name=JOB_NAME,
        )

    def test_init(self):
        """Test BeamRunPythonPipelineOperator instance is properly initialized."""
        self.assertEqual(self.operator.task_id, TASK_ID)
        self.assertEqual(self.operator.py_file, PY_FILE)
        self.assertEqual(self.operator.runner, DEFAULT_RUNNER)
        self.assertEqual(self.operator.py_options, PY_OPTIONS)
        self.assertEqual(self.operator.py_interpreter, PY_INTERPRETER)
        self.assertEqual(self.operator.default_pipeline_options, DEFAULT_OPTIONS_PYTHON)
        self.assertEqual(self.operator.pipeline_options, EXPECTED_ADDITIONAL_OPTIONS)
        self.assertEqual(self.operator.job_name, JOB_NAME)

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_direct_runner(self, gcs_hook, beam_hook_mock):
        """Test BeamHook is created and the right args are passed to
        start_python_workflow.
        """
        start_python_hook = beam_hook_mock.return_value.start_python_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        expected_options = {
            'project': 'test',
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
        }
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        start_python_hook.assert_called_once_with(
            variables=expected_options,
            py_file=mock.ANY,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_python_dataflow.
        """
        self.operator.runner = "DataflowRunner"
        start_python_hook = dataflow_mock.return_value.start_python_dataflow
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        dataflow_mock.assert_called_once_with(gcp_conn_id=self.operator.gcp_conn_id)
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
        )

    #
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_dataflow_runner(self, _, dataflow_mock):
        self.operator.runner = "DataflowRunner"
        self.operator.job_id = JOB_ID
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_called_once_with(job_id=JOB_ID, project_id=None)

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_not_called()


class TestBeamRunJavaPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.operator = BeamRunJavaPipelineOperator(
            task_id=TASK_ID,
            jar=JAR_FILE,
            job_name=JOB_NAME,
            job_class=JOB_CLASS,
            default_pipeline_options=DEFAULT_OPTIONS_JAVA,
            pipeline_options=ADDITIONAL_OPTIONS,
        )

    def test_init(self):
        """Test BeamRunJavaPipelineOperator instance is properly initialized."""
        self.assertEqual(self.operator.task_id, TASK_ID)
        self.assertEqual(self.operator.job_name, JOB_NAME)
        self.assertEqual(self.operator.runner, DEFAULT_RUNNER)
        self.assertEqual(self.operator.default_pipeline_options, DEFAULT_OPTIONS_JAVA)
        self.assertEqual(self.operator.job_class, JOB_CLASS)
        self.assertEqual(self.operator.jar, JAR_FILE)
        self.assertEqual(self.operator.pipeline_options, ADDITIONAL_OPTIONS)

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_direct_runner(self, gcs_hook, beam_hook_mock):
        """Test BeamHook is created and the right args are passed to
        start_java_workflow.
        """
        start_java_hook = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        self.assertTrue(beam_hook_mock.called)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            variables={**DEFAULT_OPTIONS_JAVA, **ADDITIONAL_OPTIONS},
            jar=mock.ANY,
            job_class=JOB_CLASS,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_dataflow.
        """
        self.operator.runner = "DataflowRunner"
        start_java_hook = dataflow_mock.return_value.start_java_dataflow
        dataflow_mock.return_value.is_job_dataflow_running.return_value = False
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        self.assertTrue(dataflow_mock.called)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables={**DEFAULT_OPTIONS_JAVA, **ADDITIONAL_OPTIONS},
            jar=mock.ANY,
            job_class=JOB_CLASS,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_dataflow_runner(self, _, dataflow_mock):
        self.operator.runner = "DataflowRunner"
        self.operator.job_id = JOB_ID
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        dataflow_mock.return_value.is_job_dataflow_running.return_value = False
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_called_once_with(job_id=JOB_ID, project_id=None)

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_not_called()
