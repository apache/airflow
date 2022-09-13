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

import unittest
from unittest import mock
from unittest.mock import MagicMock

from airflow.providers.apache.beam.operators.beam import (
    BeamRunGoPipelineOperator,
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
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
GO_FILE = 'gs://my-bucket/example/main.go'
DEFAULT_OPTIONS_PYTHON = DEFAULT_OPTIONS_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
}
ADDITIONAL_OPTIONS = {'output': 'gs://test/output', 'labels': {'foo': 'bar'}}
TEST_VERSION = f"v{version.replace('.', '-').replace('+', '-')}"
EXPECTED_ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
}
TEST_IMPERSONATION_ACCOUNT = "test@impersonation.com"


class TestBeamRunPythonPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.operator = BeamRunPythonPipelineOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            default_pipeline_options=DEFAULT_OPTIONS_PYTHON,
            pipeline_options=ADDITIONAL_OPTIONS,
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
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=None,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock):
        """Test DataflowHook is created and the right args are passed to
        start_python_dataflow.
        """
        dataflow_config = DataflowConfiguration(impersonation_chain=TEST_IMPERSONATION_ACCOUNT)
        self.operator.runner = "DataflowRunner"
        self.operator.dataflow_config = dataflow_config
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            delegate_to=dataflow_config.delegate_to,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        expected_options = {
            'project': dataflow_hook_mock.return_value.project_id,
            'job_name': job_name,
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
            'region': 'us-central1',
            'impersonate_service_account': TEST_IMPERSONATION_ACCOUNT,
        }
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        persist_link_mock.assert_called_once_with(
            self.operator,
            None,
            expected_options['project'],
            expected_options['region'],
            self.operator.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_python_pipeline.assert_called_once_with(
            variables=expected_options,
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=mock.ANY,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=self.operator.dataflow_job_id,
            job_name=job_name,
            location='us-central1',
            multiple_jobs=False,
            project_id=dataflow_config.project_id,
        )
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        self.operator.runner = "DataflowRunner"
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.dataflow_job_id = JOB_ID
        self.operator.on_kill()
        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=self.operator.dataflow_config.project_id
        )

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
            job_class=JOB_CLASS,
            default_pipeline_options=DEFAULT_OPTIONS_JAVA,
            pipeline_options=ADDITIONAL_OPTIONS,
        )

    def test_init(self):
        """Test BeamRunJavaPipelineOperator instance is properly initialized."""
        self.assertEqual(self.operator.task_id, TASK_ID)
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
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            variables={**DEFAULT_OPTIONS_JAVA, **ADDITIONAL_OPTIONS},
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=None,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_dataflow.
        """
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        self.operator.runner = "DataflowRunner"
        self.operator.dataflow_config = dataflow_config
        gcs_provide_file = gcs_hook.return_value.provide_file
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False
        self.operator.execute(None)
        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            delegate_to=dataflow_config.delegate_to,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)

        expected_options = {
            'project': dataflow_hook_mock.return_value.project_id,
            'jobName': job_name,
            'stagingLocation': 'gs://test/staging',
            'region': 'us-central1',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
            'output': 'gs://test/output',
            'impersonateServiceAccount': TEST_IMPERSONATION_ACCOUNT,
        }
        persist_link_mock.assert_called_once_with(
            self.operator,
            None,
            expected_options['project'],
            expected_options['region'],
            self.operator.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_java_pipeline.assert_called_once_with(
            variables=expected_options,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock.ANY,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=self.operator.dataflow_job_id,
            job_name=job_name,
            location='us-central1',
            multiple_jobs=False,
            project_id=dataflow_hook_mock.return_value.project_id,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        self.operator.runner = "DataflowRunner"
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.dataflow_job_id = JOB_ID
        self.operator.on_kill()
        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=self.operator.dataflow_config.project_id
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_not_called()


class TestBeamRunGoPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.operator = BeamRunGoPipelineOperator(
            task_id=TASK_ID,
            go_file=GO_FILE,
            default_pipeline_options=DEFAULT_OPTIONS_PYTHON,
            pipeline_options=ADDITIONAL_OPTIONS,
        )

    def test_init(self):
        """Test BeamRunGoPipelineOperator instance is properly initialized."""
        self.assertEqual(self.operator.task_id, TASK_ID)
        self.assertEqual(self.operator.go_file, GO_FILE)
        self.assertEqual(self.operator.runner, DEFAULT_RUNNER)
        self.assertEqual(self.operator.default_pipeline_options, DEFAULT_OPTIONS_PYTHON)
        self.assertEqual(self.operator.pipeline_options, EXPECTED_ADDITIONAL_OPTIONS)

    @mock.patch(
        "tempfile.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value='/tmp/apache-beam-go')),
    )
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_direct_runner(self, gcs_hook, beam_hook_mock, _):
        """Test BeamHook is created and the right args are passed to
        start_go_workflow.
        """
        start_go_pipeline_method = beam_hook_mock.return_value.start_go_pipeline
        gcs_provide_file_method = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        expected_options = {
            'project': 'test',
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
        }
        gcs_provide_file_method.assert_called_once_with(object_url=GO_FILE, dir="/tmp/apache-beam-go")
        start_go_pipeline_method.assert_called_once_with(
            variables=expected_options,
            go_file=gcs_provide_file_method.return_value.__enter__.return_value.name,
            process_line_callback=None,
            should_init_module=True,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.google.go_module_utils.init_module')
    def test_exec_source_on_local_path(self, init_module, beam_hook_mock):
        """
        Check that start_go_pipeline is called without initializing the Go module when source is locale.
        """
        local_go_file_path = '/tmp/file/path/example.go'
        operator = BeamRunGoPipelineOperator(
            task_id=TASK_ID,
            go_file=local_go_file_path,
        )
        start_go_pipeline_method = beam_hook_mock.return_value.start_go_pipeline
        operator.execute(None)
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        init_module.assert_not_called()
        start_go_pipeline_method.assert_called_once_with(
            variables={'labels': {'airflow-version': TEST_VERSION}},
            go_file=local_go_file_path,
            process_line_callback=None,
            should_init_module=False,
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch(
        "tempfile.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value='/tmp/apache-beam-go')),
    )
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, _, persist_link_mock):
        """Test DataflowHook is created and the right args are passed to
        start_go_dataflow.
        """
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        self.operator.runner = "DataflowRunner"
        self.operator.dataflow_config = dataflow_config
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.operator.execute(None)
        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            delegate_to=dataflow_config.delegate_to,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        expected_options = {
            'project': dataflow_hook_mock.return_value.project_id,
            'job_name': job_name,
            'staging_location': 'gs://test/staging',
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
            'region': 'us-central1',
        }
        persist_link_mock.assert_called_once_with(
            self.operator,
            None,
            expected_options['project'],
            expected_options['region'],
            self.operator.dataflow_job_id,
        )
        gcs_provide_file.assert_called_once_with(object_url=GO_FILE, dir='/tmp/apache-beam-go')
        beam_hook_mock.return_value.start_go_pipeline.assert_called_once_with(
            variables=expected_options,
            go_file=gcs_provide_file.return_value.__enter__.return_value.name,
            process_line_callback=mock.ANY,
            should_init_module=True,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=self.operator.dataflow_job_id,
            job_name=job_name,
            location='us-central1',
            multiple_jobs=False,
            project_id=dataflow_config.project_id,
        )
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowJobLink.persist')
    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        self.operator.runner = "DataflowRunner"
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.dataflow_job_id = JOB_ID
        self.operator.on_kill()
        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=self.operator.dataflow_config.project_id
        )

    @mock.patch('airflow.providers.apache.beam.operators.beam.BeamHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.DataflowHook')
    @mock.patch('airflow.providers.apache.beam.operators.beam.GCSHook')
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        self.operator.execute(None)
        self.operator.on_kill()
        dataflow_cancel_job.assert_not_called()
