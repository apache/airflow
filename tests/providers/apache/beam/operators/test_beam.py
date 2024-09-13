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

import copy
import os
from unittest import mock
from unittest.mock import MagicMock, call

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.apache.beam.operators.beam import (
    BeamBasePipelineOperator,
    BeamRunGoPipelineOperator,
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)
from airflow.providers.apache.beam.triggers.beam import BeamJavaPipelineTrigger, BeamPythonPipelineTrigger
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.version import version

TASK_ID = "test-beam-operator"
DEFAULT_RUNNER = "DirectRunner"
JOB_ID = "test-dataflow-pipeline-id"
JAR_FILE = "gs://my-bucket/example/test.jar"
JOB_CLASS = "com.test.NotMain"
PY_FILE = "gs://my-bucket/my-object.py"
REQURIEMENTS_FILE = "gs://my-bucket/my-requirements.txt"
PY_INTERPRETER = "python3"
GO_FILE = "gs://my-bucket/example/main.go"
LAUNCHER_BINARY = "gs://my-bucket/example/launcher"
WORKER_BINARY = "gs://my-bucket/example/worker"
TEST_PROJECT = "test"
STAGING_LOCATION = "gs://test/staging"
OUTPUT_LOCATION = "gs://test/output"
TEST_VERSION = f"v{version.replace('.', '-').replace('+', '-')}"
TEST_IMPERSONATION_ACCOUNT = "test@impersonation.com"
BEAM_OPERATOR_PATH = "airflow.providers.apache.beam.operators.beam.{}"


@pytest.fixture
def default_options():
    return {"project": TEST_PROJECT, "stagingLocation": STAGING_LOCATION}


@pytest.fixture
def py_default_options(default_options):
    return {
        **default_options,
        "requirements_file": REQURIEMENTS_FILE,
    }


@pytest.fixture
def py_options():
    return ["-m"]


@pytest.fixture
def pipeline_options():
    return {"output": OUTPUT_LOCATION, "labels": {"foo": "bar"}}


class TestBeamBasePipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {"task_id": TASK_ID, "runner": DEFAULT_RUNNER}

    def test_init(self, default_options, pipeline_options):
        op = BeamBasePipelineOperator(
            **self.default_op_kwargs,
            default_pipeline_options=copy.deepcopy(default_options),
            pipeline_options=copy.deepcopy(pipeline_options),
            dataflow_config={},
        )
        # Should not change into the operator constructor, it might define in templated_fields
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}

    def test_async_execute_should_throw_exception(self):
        """Tests that an AirflowException is raised in case of error event"""
        op = BeamBasePipelineOperator(**self.default_op_kwargs)
        with pytest.raises(AirflowException):
            op.execute_complete(
                context=mock.MagicMock(), event={"status": "error", "message": "test failure message"}
            )

    def test_async_execute_logging_should_execute_successfully(self, caplog):
        """Asserts that logging occurs as expected"""
        op = BeamBasePipelineOperator(**self.default_op_kwargs)
        op.execute_complete(
            context=mock.MagicMock(),
            event={"status": "success", "message": "Pipeline has finished SUCCESSFULLY"},
        )
        assert f"{TASK_ID} completed with response Pipeline has finished SUCCESSFULLY" in caplog.text


class TestBeamRunPythonPipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, py_options, py_default_options, pipeline_options):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "py_file": PY_FILE,
            "py_options": copy.deepcopy(py_options),
            "default_pipeline_options": copy.deepcopy(py_default_options),
            "pipeline_options": copy.deepcopy(pipeline_options),
        }

    def test_init(self, py_default_options, py_options, pipeline_options):
        """Test BeamRunPythonPipelineOperator instance is properly initialized."""
        op = BeamRunPythonPipelineOperator(
            **self.default_op_kwargs,
            dataflow_config={},
        )
        assert op.py_file == PY_FILE
        assert op.runner == DEFAULT_RUNNER
        assert op.py_options == py_options
        assert op.py_interpreter == PY_INTERPRETER
        # Should not change into the operator constructor, it might define in templated_fields
        assert op.default_pipeline_options == py_default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_direct_runner(self, gcs_hook, beam_hook_mock, py_options):
        """Test BeamHook is created and the right args are passed to
        start_python_workflow.
        """
        start_python_hook = beam_hook_mock.return_value.start_python_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        op = BeamRunPythonPipelineOperator(**self.default_op_kwargs)

        op.execute({})
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        expected_options = {
            "project": "test",
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
            "requirements_file": gcs_provide_file.return_value.__enter__.return_value.name,
        }
        gcs_provide_file.assert_any_call(object_url=PY_FILE)
        gcs_provide_file.assert_any_call(object_url=REQURIEMENTS_FILE)
        start_python_hook.assert_called_once_with(
            variables=expected_options,
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=py_options,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=None,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock, py_options
    ):
        """Test DataflowHook is created and the right args are passed to
        start_python_dataflow.
        """
        gcs_provide_file = gcs_hook.return_value.provide_file
        op = BeamRunPythonPipelineOperator(
            dataflow_config={"impersonation_chain": TEST_IMPERSONATION_ACCOUNT},
            runner="DataflowRunner",
            **self.default_op_kwargs,
        )

        op.execute({})

        assert op.dataflow_config.gcp_conn_id == "google_cloud_default"
        assert op.dataflow_config.impersonation_chain == TEST_IMPERSONATION_ACCOUNT
        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            poll_sleep=op.dataflow_config.poll_sleep,
            impersonation_chain=TEST_IMPERSONATION_ACCOUNT,
            drain_pipeline=op.dataflow_config.drain_pipeline,
            cancel_timeout=op.dataflow_config.cancel_timeout,
            wait_until_finished=op.dataflow_config.wait_until_finished,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "job_name": job_name,
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
            "region": "us-central1",
            "impersonate_service_account": TEST_IMPERSONATION_ACCOUNT,
            "requirements_file": gcs_provide_file.return_value.__enter__.return_value.name,
        }
        gcs_provide_file.assert_any_call(object_url=PY_FILE)
        gcs_provide_file.assert_any_call(object_url=REQURIEMENTS_FILE)
        persist_link_mock.assert_called_once_with(
            op,
            {},
            expected_options["project"],
            expected_options["region"],
            op.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_python_pipeline.assert_called_once_with(
            variables=expected_options,
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=py_options,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=mock.ANY,
            check_job_status_callback=mock.ANY,
        )
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner__no_dataflow_job_name(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock
    ):
        """Test that the task_id is passed as the Dataflow job name if not set in dataflow_config."""
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        op = BeamRunPythonPipelineOperator(
            **self.default_op_kwargs, dataflow_config=dataflow_config, runner="DataflowRunner"
        )
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False
        op.execute({})
        assert op.dataflow_config.job_name == op.task_id

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        op = BeamRunPythonPipelineOperator(runner="DataflowRunner", **self.default_op_kwargs)
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job

        op.execute({})
        op.dataflow_job_id = JOB_ID
        op.on_kill()

        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=op.dataflow_config.project_id, location=op.dataflow_config.location
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        op = BeamRunPythonPipelineOperator(**self.default_op_kwargs)

        op.execute({})
        op.on_kill()
        dataflow_cancel_job.assert_not_called()

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_execute_gcs_hook_not_called_without_gs_prefix(self, mock_gcs_hook, _):
        """
        Test that execute method does not call GCSHook when neither py_file nor requirements_file
        starts with 'gs://'. (i.e., running pipeline entirely locally)
        """
        local_test_op_args = {
            "task_id": TASK_ID,
            "py_file": "local_file.py",
            "py_options": ["-m"],
            "default_pipeline_options": {
                "project": TEST_PROJECT,
                "requirements_file": "local_requirements.txt",
            },
            "pipeline_options": {"output": "test_local/output", "labels": {"foo": "bar"}},
        }

        op = BeamRunPythonPipelineOperator(**local_test_op_args)
        context_mock = mock.MagicMock()

        op.execute(context_mock)
        mock_gcs_hook.assert_not_called()

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_execute_gcs_hook_called_with_gs_prefix_py_file(self, mock_gcs_hook, _):
        """
        Test that execute method calls GCSHook when only 'py_file' starts with 'gs://'.
        """
        local_test_op_args = {
            "task_id": TASK_ID,
            "py_file": "gs://gcs_file.py",
            "py_options": ["-m"],
            "default_pipeline_options": {
                "project": TEST_PROJECT,
                "requirements_file": "local_requirements.txt",
            },
            "pipeline_options": {"output": "test_local/output", "labels": {"foo": "bar"}},
        }
        op = BeamRunPythonPipelineOperator(**local_test_op_args)
        context_mock = mock.MagicMock()

        op.execute(context_mock)
        mock_gcs_hook.assert_called_once()

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_execute_gcs_hook_called_with_gs_prefix_pipeline_requirements(self, mock_gcs_hook, _):
        """
        Test that execute method calls GCSHook when only pipeline_options 'requirements_file' starts with
        'gs://'.
        Note: "pipeline_options" is merged with and overrides keys in "default_pipeline_options" when
              BeamRunPythonPipelineOperator is instantiated, so testing GCS 'requirements_file' specified
              in "pipeline_options"
        """
        local_test_op_args = {
            "task_id": TASK_ID,
            "py_file": "local_file.py",
            "py_options": ["-m"],
            "default_pipeline_options": {
                "project": TEST_PROJECT,
                "requirements_file": "gs://gcs_requirements.txt",
            },
            "pipeline_options": {"output": "test_local/output", "labels": {"foo": "bar"}},
        }

        op = BeamRunPythonPipelineOperator(**local_test_op_args)
        context_mock = mock.MagicMock()

        op.execute(context_mock)
        mock_gcs_hook.assert_called_once()


class TestBeamRunJavaPipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, default_options, pipeline_options):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "jar": JAR_FILE,
            "job_class": JOB_CLASS,
            "default_pipeline_options": copy.deepcopy(default_options),
            "pipeline_options": copy.deepcopy(pipeline_options),
        }

    def test_init(self, default_options, pipeline_options):
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs, dataflow_config={})
        # Should not change into the operator constructor, it might define in templated_fields
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}
        assert op.job_class == JOB_CLASS
        assert op.jar == JAR_FILE

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_direct_runner(self, gcs_hook, beam_hook_mock, default_options, pipeline_options):
        """Test BeamHook is created and the right args are passed to
        start_java_workflow.
        """
        start_java_hook = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs)

        op.execute({})

        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        start_java_hook.assert_called_once_with(
            variables={**default_options, **pipeline_options},
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=None,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_dataflow.
        """
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        op = BeamRunJavaPipelineOperator(
            **self.default_op_kwargs, dataflow_config=dataflow_config, runner="DataflowRunner"
        )
        gcs_provide_file = gcs_hook.return_value.provide_file
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False

        op.execute({})

        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)

        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "jobName": job_name,
            "stagingLocation": "gs://test/staging",
            "region": "us-central1",
            "labels": {"foo": "bar"},
            "output": "gs://test/output",
            "impersonateServiceAccount": TEST_IMPERSONATION_ACCOUNT,
        }
        persist_link_mock.assert_called_once_with(
            op,
            {},
            expected_options["project"],
            expected_options["region"],
            op.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_java_pipeline.assert_called_once_with(
            variables=expected_options,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock.ANY,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner__no_dataflow_job_name(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock
    ):
        """Test that the task_id is passed as the Dataflow job name if not set in dataflow_config."""
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        op = BeamRunJavaPipelineOperator(
            **self.default_op_kwargs, dataflow_config=dataflow_config, runner="DataflowRunner"
        )
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False
        op.execute({})
        assert op.dataflow_config.job_name == op.task_id

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        dataflow_hook_mock.return_value.is_job_dataflow_running.return_value = False
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs, runner="DataflowRunner")

        op.execute({})
        op.dataflow_job_id = JOB_ID
        op.on_kill()

        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=op.dataflow_config.project_id, location=op.dataflow_config.location
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs)

        op.execute(None)
        op.on_kill()

        dataflow_cancel_job.assert_not_called()


class TestBeamRunGoPipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, default_options, pipeline_options):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "default_pipeline_options": copy.deepcopy(default_options),
            "pipeline_options": copy.deepcopy(pipeline_options),
        }

    def test_init(self, default_options, pipeline_options):
        """Test BeamRunGoPipelineOperator instance is properly initialized with go_file."""
        op = BeamRunGoPipelineOperator(**self.default_op_kwargs, go_file=GO_FILE, dataflow_config={})
        assert op.task_id == TASK_ID
        assert op.go_file == GO_FILE
        assert op.launcher_binary == ""
        assert op.worker_binary == ""
        assert op.runner == DEFAULT_RUNNER
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}

    def test_init_with_launcher_binary(self, default_options, pipeline_options):
        """Test BeamRunGoPipelineOperator instance is properly initialized with launcher_binary."""
        op = BeamRunGoPipelineOperator(
            **self.default_op_kwargs, launcher_binary=LAUNCHER_BINARY, dataflow_config={}
        )

        assert op.task_id == TASK_ID
        assert op.go_file == ""
        assert op.launcher_binary == LAUNCHER_BINARY
        assert op.worker_binary == LAUNCHER_BINARY
        assert op.runner == DEFAULT_RUNNER
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}

    def test_init_with_launcher_binary_and_worker_binary(self, default_options, pipeline_options):
        """
        Test BeamRunGoPipelineOperator instance is properly initialized with launcher_binary and
        worker_binary.
        """
        op = BeamRunGoPipelineOperator(
            **self.default_op_kwargs,
            launcher_binary=LAUNCHER_BINARY,
            worker_binary=WORKER_BINARY,
            dataflow_config={},
        )

        assert op.task_id == TASK_ID
        assert op.go_file == ""
        assert op.launcher_binary == LAUNCHER_BINARY
        assert op.worker_binary == WORKER_BINARY
        assert op.runner == DEFAULT_RUNNER
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}

    @pytest.mark.parametrize(
        "launcher_binary, go_file",
        [
            pytest.param("", "", id="both-empty"),
            pytest.param(None, None, id="both-not-set"),
            pytest.param(LAUNCHER_BINARY, GO_FILE, id="both-set"),
        ],
    )
    def test_init_with_neither_go_file_nor_launcher_binary_raises(self, launcher_binary, go_file):
        """
        Test BeamRunGoPipelineOperator initialization raises ValueError when neither
        go_file nor launcher_binary is provided.
        """
        op = BeamRunGoPipelineOperator(
            **self.default_op_kwargs, launcher_binary=launcher_binary, go_file=go_file
        )
        with pytest.raises(ValueError, match="Exactly one of `go_file` and `launcher_binary` must be set"):
            op.execute({})

    @mock.patch(
        "tempfile.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value="/tmp/apache-beam-go")),
    )
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_direct_runner_with_gcs_go_file(self, gcs_hook, beam_hook_mock, _):
        """Test BeamHook is created and the right args are passed to
        start_go_workflow.
        """
        start_go_pipeline_method = beam_hook_mock.return_value.start_go_pipeline
        gcs_download_method = gcs_hook.return_value.download
        op = BeamRunGoPipelineOperator(**self.default_op_kwargs, go_file=GO_FILE)

        op.execute({})
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        expected_options = {
            "project": "test",
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
        }
        expected_go_file = "/tmp/apache-beam-go/main.go"
        gcs_download_method.assert_called_once_with(
            bucket_name="my-bucket", object_name="example/main.go", filename=expected_go_file
        )
        start_go_pipeline_method.assert_called_once_with(
            variables=expected_options,
            go_file=expected_go_file,
            process_line_callback=None,
            should_init_module=True,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch("tempfile.TemporaryDirectory")
    def test_exec_direct_runner_with_gcs_launcher_binary(
        self, mock_tmp_dir, mock_beam_hook, mock_gcs_hook, tmp_path
    ):
        """
        Test start_go_pipeline_from_binary is called with an executable launcher binary downloaded from GCS.
        """

        def tmp_dir_side_effect(prefix: str) -> str:
            sub_dir = tmp_path / mock_tmp_dir.call_args.kwargs["prefix"]
            sub_dir.mkdir()
            return str(sub_dir)

        mock_tmp_dir.return_value.__enter__.side_effect = tmp_dir_side_effect

        def gcs_download_side_effect(bucket_name: str, object_name: str, filename: str) -> None:
            open(filename, "wb").close()

        gcs_download_method = mock_gcs_hook.return_value.download
        gcs_download_method.side_effect = gcs_download_side_effect

        start_go_pipeline_method = mock_beam_hook.return_value.start_go_pipeline_with_binary

        op = BeamRunGoPipelineOperator(**self.default_op_kwargs, launcher_binary="gs://bucket/path/to/main")
        op.execute({})

        expected_binary = f"{tmp_path}/apache-beam-go/launcher-main"
        expected_options = {
            "project": "test",
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
        }
        mock_beam_hook.assert_called_once_with(runner=DEFAULT_RUNNER)
        mock_tmp_dir.assert_called_once_with(prefix="apache-beam-go")
        gcs_download_method.assert_called_once_with(
            bucket_name="bucket",
            object_name="path/to/main",
            filename=expected_binary,
        )
        assert os.access(expected_binary, os.X_OK)
        start_go_pipeline_method.assert_called_once_with(
            variables=expected_options,
            launcher_binary=expected_binary,
            worker_binary=expected_binary,
            process_line_callback=None,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch("airflow.providers.google.go_module_utils.init_module")
    def test_exec_direct_runner_with_local_go_file(self, init_module, beam_hook_mock):
        """
        Check that start_go_pipeline is called without initializing the Go module when source is locale.
        """
        local_go_file_path = "/tmp/file/path/example.go"
        operator = BeamRunGoPipelineOperator(
            task_id=TASK_ID,
            go_file=local_go_file_path,
        )
        start_go_pipeline_method = beam_hook_mock.return_value.start_go_pipeline
        operator.execute({})
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        init_module.assert_not_called()
        start_go_pipeline_method.assert_called_once_with(
            variables={"labels": {"airflow-version": TEST_VERSION}},
            go_file=local_go_file_path,
            process_line_callback=None,
            should_init_module=False,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    def test_exec_direct_runner_with_local_launcher_binary(self, mock_beam_hook):
        """
        Test start_go_pipeline_with_binary is called with a local launcher binary.
        """
        start_go_pipeline_method = mock_beam_hook.return_value.start_go_pipeline_with_binary

        operator = BeamRunGoPipelineOperator(
            task_id=TASK_ID,
            launcher_binary="/local/path/to/main",
        )
        operator.execute({})

        expected_binary = "/local/path/to/main"

        mock_beam_hook.assert_called_once_with(runner=DEFAULT_RUNNER)
        start_go_pipeline_method.assert_called_once_with(
            variables={"labels": {"airflow-version": TEST_VERSION}},
            launcher_binary=expected_binary,
            worker_binary=expected_binary,
            process_line_callback=None,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(
        "tempfile.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value="/tmp/apache-beam-go")),
    )
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner_with_go_file(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, _, persist_link_mock
    ):
        """Test DataflowHook is created and the right args are passed to
        start_go_dataflow.
        """
        gcs_download_method = gcs_hook.return_value.download
        dataflow_config = DataflowConfiguration(impersonation_chain="test@impersonation.com")
        op = BeamRunGoPipelineOperator(
            runner="DataflowRunner",
            dataflow_config=dataflow_config,
            go_file=GO_FILE,
            **self.default_op_kwargs,
        )

        op.execute({})

        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "job_name": job_name,
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
            "region": "us-central1",
        }
        persist_link_mock.assert_called_once_with(
            op,
            {},
            expected_options["project"],
            expected_options["region"],
            op.dataflow_job_id,
        )
        expected_go_file = "/tmp/apache-beam-go/main.go"
        gcs_download_method.assert_called_once_with(
            bucket_name="my-bucket", object_name="example/main.go", filename=expected_go_file
        )
        beam_hook_mock.return_value.start_go_pipeline.assert_called_once_with(
            variables=expected_options,
            go_file=expected_go_file,
            process_line_callback=mock.ANY,
            should_init_module=True,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=op.dataflow_job_id,
            job_name=job_name,
            location="us-central1",
            multiple_jobs=False,
            project_id=dataflow_config.project_id,
        )
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("tempfile.TemporaryDirectory"))
    def test_exec_dataflow_runner_with_launcher_binary_and_worker_binary(
        self, mock_tmp_dir, mock_beam_hook, mock_gcs_hook, mock_dataflow_hook, mock_persist_link, tmp_path
    ):
        """
        Test DataflowHook is created and start_go_pipeline_from_binary is called with
        a launcher binary and a worker binary.
        """

        def tmp_dir_side_effect(prefix: str) -> str:
            sub_dir = tmp_path / mock_tmp_dir.call_args.kwargs["prefix"]
            sub_dir.mkdir()
            return str(sub_dir)

        mock_tmp_dir.return_value.__enter__.side_effect = tmp_dir_side_effect

        def gcs_download_side_effect(bucket_name: str, object_name: str, filename: str) -> None:
            open(filename, "wb").close()

        gcs_download_method = mock_gcs_hook.return_value.download
        gcs_download_method.side_effect = gcs_download_side_effect

        mock_dataflow_hook.build_dataflow_job_name.return_value = "test-job"

        provide_authorized_gcloud_method = mock_dataflow_hook.return_value.provide_authorized_gcloud
        start_go_pipeline_method = mock_beam_hook.return_value.start_go_pipeline_with_binary
        wait_for_done_method = mock_dataflow_hook.return_value.wait_for_done

        dataflow_config = DataflowConfiguration(project_id="test-project")

        operator = BeamRunGoPipelineOperator(
            launcher_binary="gs://bucket/path/to/main1",
            worker_binary="gs://bucket/path/to/main2",
            runner="DataflowRunner",
            dataflow_config=dataflow_config,
            **self.default_op_kwargs,
        )
        operator.execute({})

        expected_launcher_binary = str(tmp_path / "apache-beam-go/launcher-main1")
        expected_worker_binary = str(tmp_path / "apache-beam-go/worker-main2")
        expected_job_name = "test-job"
        expected_options = {
            "project": "test-project",
            "job_name": expected_job_name,
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
            "region": "us-central1",
        }

        mock_tmp_dir.assert_called_once_with(prefix="apache-beam-go")
        gcs_download_method.assert_has_calls(
            [
                call(bucket_name="bucket", object_name="path/to/main1", filename=expected_launcher_binary),
                call(bucket_name="bucket", object_name="path/to/main2", filename=expected_worker_binary),
            ],
        )
        assert os.access(expected_launcher_binary, os.X_OK)
        assert os.access(expected_worker_binary, os.X_OK)

        mock_dataflow_hook.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        provide_authorized_gcloud_method.assert_called_once_with()
        start_go_pipeline_method.assert_called_once_with(
            variables=expected_options,
            launcher_binary=expected_launcher_binary,
            worker_binary=expected_worker_binary,
            process_line_callback=mock.ANY,
        )
        mock_persist_link.assert_called_once_with(
            operator,
            {},
            dataflow_config.project_id,
            dataflow_config.location,
            operator.dataflow_job_id,
        )
        wait_for_done_method.assert_called_once_with(
            job_name=expected_job_name,
            location=dataflow_config.location,
            job_id=operator.dataflow_job_id,
            multiple_jobs=False,
            project_id=dataflow_config.project_id,
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        op = BeamRunGoPipelineOperator(**self.default_op_kwargs, go_file=GO_FILE, runner="DataflowRunner")

        op.execute({})
        op.dataflow_job_id = JOB_ID
        op.on_kill()

        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=op.dataflow_config.project_id, location=op.dataflow_config.location
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        op = BeamRunGoPipelineOperator(**self.default_op_kwargs, go_file=GO_FILE)

        op.execute({})
        op.on_kill()
        dataflow_cancel_job.assert_not_called()


class TestBeamRunPythonPipelineOperatorAsync:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, default_options, pipeline_options, py_options):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "py_file": PY_FILE,
            "py_options": copy.deepcopy(py_options),
            "default_pipeline_options": copy.deepcopy(default_options),
            "pipeline_options": copy.deepcopy(pipeline_options),
            "deferrable": True,
        }

    def test_init(self, default_options, pipeline_options, py_options):
        """Test BeamRunPythonPipelineOperator instance is properly initialized."""
        op = BeamRunPythonPipelineOperator(**self.default_op_kwargs, dataflow_config={})

        assert op.task_id == TASK_ID
        assert op.runner == DEFAULT_RUNNER
        assert op.py_file == PY_FILE
        assert op.py_interpreter == PY_INTERPRETER
        assert op.py_options == py_options
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}
        assert op.deferrable is True

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_async_execute_should_execute_successfully(self, gcs_hook, beam_hook_mock):
        """
        Asserts that a task is deferred and the BeamPythonPipelineTrigger will be fired
        when the BeamRunPythonPipelineOperator is executed in deferrable mode when deferrable=True.
        """
        op = BeamRunPythonPipelineOperator(**self.default_op_kwargs)
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=mock.MagicMock())

        assert isinstance(
            exc.value.trigger, BeamPythonPipelineTrigger
        ), "Trigger is not a BeamPythonPipelineTrigger"

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_async_execute_direct_runner(self, gcs_hook, beam_hook_mock):
        """
        Test BeamHook is created and the right args are passed to
        start_python_workflow when executing direct runner.
        """
        gcs_provide_file = gcs_hook.return_value.provide_file
        op = BeamRunPythonPipelineOperator(**self.default_op_kwargs)
        with pytest.raises(TaskDeferred):
            op.execute(context=mock.MagicMock())
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_exec_dataflow_runner(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, persist_link_mock):
        """
        Test DataflowHook is created and the right args are passed to
        start_python_dataflow when executing Dataflow runner.
        """

        dataflow_config = DataflowConfiguration(impersonation_chain=TEST_IMPERSONATION_ACCOUNT)
        op = BeamRunPythonPipelineOperator(
            runner="DataflowRunner",
            dataflow_config=dataflow_config,
            **self.default_op_kwargs,
        )
        gcs_provide_file = gcs_hook.return_value.provide_file
        magic_mock = mock.MagicMock()
        with pytest.raises(TaskDeferred):
            op.execute(context=magic_mock)

        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "job_name": job_name,
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
            "region": "us-central1",
            "impersonate_service_account": TEST_IMPERSONATION_ACCOUNT,
        }
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        persist_link_mock.assert_called_once_with(
            op,
            magic_mock,
            expected_options["project"],
            expected_options["region"],
            op.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_python_pipeline.assert_not_called()
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        op = BeamRunPythonPipelineOperator(runner="DataflowRunner", **self.default_op_kwargs)
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        with pytest.raises(TaskDeferred):
            op.execute(context=mock.MagicMock())
        op.dataflow_job_id = JOB_ID
        op.on_kill()
        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=op.dataflow_config.project_id, location=op.dataflow_config.location
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        op = BeamRunPythonPipelineOperator(runner="DataflowRunner", **self.default_op_kwargs)
        with pytest.raises(TaskDeferred):
            op.execute(mock.MagicMock())
        op.on_kill()
        dataflow_cancel_job.assert_not_called()


class TestBeamRunJavaPipelineOperatorAsync:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, default_options, pipeline_options):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "jar": JAR_FILE,
            "job_class": JOB_CLASS,
            "default_pipeline_options": copy.deepcopy(default_options),
            "pipeline_options": copy.deepcopy(pipeline_options),
            "deferrable": True,
        }

    def test_init(self, default_options, pipeline_options, py_options):
        """Test BeamRunJavaPipelineOperator instance is properly initialized."""
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs, dataflow_config={})
        assert op.task_id == TASK_ID
        # Should not change into the operator constructor, it might define in templated_fields
        assert op.default_pipeline_options == default_options
        assert op.pipeline_options == pipeline_options
        assert op.dataflow_config == {}
        assert op.job_class == JOB_CLASS
        assert op.jar == JAR_FILE
        assert op.deferrable is True

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_async_execute_should_execute_successfully(self, gcs_hook, beam_hook_mock):
        """
        Asserts that a task is deferred and the BeamJavaPipelineTrigger will be fired
        when the BeamRunPythonPipelineOperator is executed in deferrable mode when deferrable=True.
        """
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs)
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=mock.MagicMock())

        assert isinstance(
            exc.value.trigger, BeamJavaPipelineTrigger
        ), "Trigger is not a BeamPJavaPipelineTrigger"

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    def test_async_execute_direct_runner(self, beam_hook_mock):
        """
        Test BeamHook is created and the right args are passed to
        start_java_pipeline when executing direct runner.
        """
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs)
        with pytest.raises(TaskDeferred):
            op.execute(context=mock.MagicMock())
        beam_hook_mock.assert_called_once_with(runner=DEFAULT_RUNNER)

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_exec_dataflow_runner(self, dataflow_hook_mock, beam_hook_mock, persist_link_mock):
        """
        Test DataflowHook is created and the right args are passed to
        start_java_pipeline when executing Dataflow runner.
        """
        dataflow_config = DataflowConfiguration(impersonation_chain=TEST_IMPERSONATION_ACCOUNT)
        op = BeamRunJavaPipelineOperator(
            runner="DataflowRunner", dataflow_config=dataflow_config, **self.default_op_kwargs
        )
        magic_mock = mock.MagicMock()
        with pytest.raises(TaskDeferred):
            op.execute(context=magic_mock)

        job_name = dataflow_hook_mock.build_dataflow_job_name.return_value
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id=dataflow_config.gcp_conn_id,
            poll_sleep=dataflow_config.poll_sleep,
            impersonation_chain=dataflow_config.impersonation_chain,
            drain_pipeline=dataflow_config.drain_pipeline,
            cancel_timeout=dataflow_config.cancel_timeout,
            wait_until_finished=dataflow_config.wait_until_finished,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "job_name": job_name,
            "staging_location": "gs://test/staging",
            "output": "gs://test/output",
            "labels": {"foo": "bar"},
            "region": "us-central1",
            "impersonate_service_account": TEST_IMPERSONATION_ACCOUNT,
        }
        persist_link_mock.assert_called_once_with(
            op,
            magic_mock,
            expected_options["project"],
            expected_options["region"],
            op.dataflow_job_id,
        )
        beam_hook_mock.return_value.start_python_pipeline.assert_not_called()
        dataflow_hook_mock.return_value.provide_authorized_gcloud.assert_called_once_with()

    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowJobLink.persist"))
    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    def test_on_kill_dataflow_runner(self, dataflow_hook_mock, _, __, ___):
        dataflow_cancel_job = dataflow_hook_mock.return_value.cancel_job
        op = BeamRunJavaPipelineOperator(runner="DataflowRunner", **self.default_op_kwargs)
        with pytest.raises(TaskDeferred):
            op.execute(context=mock.MagicMock())
        op.dataflow_job_id = JOB_ID
        op.on_kill()
        dataflow_cancel_job.assert_called_once_with(
            job_id=JOB_ID, project_id=op.dataflow_config.project_id, location=op.dataflow_config.location
        )

    @mock.patch(BEAM_OPERATOR_PATH.format("BeamHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("DataflowHook"))
    @mock.patch(BEAM_OPERATOR_PATH.format("GCSHook"))
    def test_on_kill_direct_runner(self, _, dataflow_mock, __):
        dataflow_cancel_job = dataflow_mock.return_value.cancel_job
        op = BeamRunJavaPipelineOperator(**self.default_op_kwargs)
        with pytest.raises(TaskDeferred):
            op.execute(mock.MagicMock())
        op.on_kill()
        dataflow_cancel_job.assert_not_called()
