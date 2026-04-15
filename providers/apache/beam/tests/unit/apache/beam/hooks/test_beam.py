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
import logging
import os
import re
import subprocess
import sys
from importlib.metadata import version as importlib_version
from unittest import mock
from unittest.mock import ANY, AsyncMock, MagicMock, call

import pytest

from airflow.providers.apache.beam.hooks.beam import (
    BeamAsyncHook,
    BeamHook,
    beam_options_to_args,
    process_fd,
    run_beam_command,
)
from airflow.providers.common.compat.sdk import AirflowException

PY_FILE = "apache_beam.examples.wordcount"
JAR_FILE = "unitest.jar"
JOB_CLASS = "com.example.UnitTest"
PY_OPTIONS = ["-m"]
TEST_JOB_ID = "test-job-id"

GO_FILE = "/path/to/file.go"
DEFAULT_RUNNER = "DirectRunner"
BEAM_STRING = "airflow.providers.apache.beam.hooks.beam.{}"
BEAM_VARIABLES = {"output": "gs://test/output", "labels": {"foo": "bar"}}
BEAM_VARIABLES_PY = {"output": "gs://test/output", "labels": {"foo": "bar"}}
BEAM_VARIABLES_JAVA = {
    "output": "gs://test/output",
    "labels": {"foo": "bar"},
}
BEAM_VARIABLES_JAVA_STRING_LABELS = {
    "output": "gs://test/output",
    "labels": '{"foo":"bar"}',
}
BEAM_VARIABLES_GO = {"output": "gs://test/output", "labels": {"foo": "bar"}}
PIPELINE_COMMAND_PREFIX = ["a", "b", "c"]
WORKING_DIRECTORY = "test_wd"

APACHE_BEAM_V_2_14_0_JAVA_SDK_LOG = f""""\
Dataflow SDK version: 2.14.0
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow\
/jobsDetail/locations/europe-west3/jobs/{TEST_JOB_ID}?project=XXX
Submitted job: {TEST_JOB_ID}
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To cancel the job using the 'gcloud' tool, run:
> gcloud dataflow jobs --project=XXX cancel --region=europe-west3 {TEST_JOB_ID}
"""

try:
    APACHE_BEAM_VERSION: str | None = importlib_version("apache-beam")
except ImportError:
    APACHE_BEAM_VERSION = None

try:
    from airflow._shared.configuration import AirflowConfigException as ConfigException
except ImportError:
    # Compat for airflow < 3.2, where AirflowConfigException is in airflow.exceptions
    ConfigException = AirflowException  # type: ignore[assignment,misc]


class TestBeamHook:
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.39.0")
    def test_start_python_pipeline(self, mock_check_output, mock_runner):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()
        is_dataflow_job_id_exist_callback = MagicMock()
        on_dataflow_job_id_found_callback = MagicMock()

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            process_line_callback=process_line_callback,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
            on_dataflow_job_id_found_callback=on_dataflow_job_id_found_callback,
        )

        expected_cmd = [
            "python3",
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=None,
            log=ANY,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
            on_dataflow_job_id_found_callback=on_dataflow_job_id_found_callback,
        )

    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.35.0")
    def test_start_python_pipeline_unsupported_option(self, mock_check_output):
        hook = BeamHook(runner=DEFAULT_RUNNER)

        with pytest.raises(
            AirflowException,
            match=re.escape("The impersonateServiceAccount option requires Apache Beam 2.39.0 or newer."),
        ):
            hook.start_python_pipeline(
                variables={
                    "impersonate_service_account": "test@impersonation.com",
                },
                py_file="/tmp/file.py",
                py_options=["-m"],
                py_interpreter="python3",
                py_requirements=None,
                py_system_site_packages=False,
                process_line_callback=MagicMock(),
                is_dataflow_job_id_exist_callback=MagicMock(),
            )

    @pytest.mark.parametrize(
        "py_interpreter",
        [
            pytest.param("python", id="default python"),
            pytest.param("python2", id="major python version 2.x"),
            pytest.param("python3", id="major python version 3.x"),
            pytest.param("python3.6", id="major.minor python version"),
        ],
    )
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.39.0")
    def test_start_python_pipeline_with_custom_interpreter(
        self, mock_check_output, mock_runner, py_interpreter
    ):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()
        is_dataflow_job_id_exist_callback = MagicMock()

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_interpreter=py_interpreter,
            process_line_callback=process_line_callback,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
        )

        expected_cmd = [
            py_interpreter,
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=None,
            log=ANY,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
            on_dataflow_job_id_found_callback=None,
        )

    @pytest.mark.parametrize(
        ("current_py_requirements", "current_py_system_site_packages"),
        [
            pytest.param("foo-bar", False, id="requirements without system site-packages"),
            pytest.param("foo-bar", True, id="requirements with system site-packages"),
            pytest.param([], True, id="only system site-packages"),
        ],
    )
    @mock.patch(BEAM_STRING.format("prepare_virtualenv"))
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.39.0")
    def test_start_python_pipeline_with_non_empty_py_requirements_and_without_system_packages(
        self,
        mock_check_output,
        mock_runner,
        mock_virtualenv,
        current_py_requirements,
        current_py_system_site_packages,
    ):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        mock_virtualenv.return_value = "/dummy_dir/bin/python"
        process_line_callback = MagicMock()
        is_dataflow_job_id_exist_callback = MagicMock()

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_requirements=current_py_requirements,
            py_system_site_packages=current_py_system_site_packages,
            process_line_callback=process_line_callback,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
        )

        expected_cmd = [
            "/dummy_dir/bin/python",
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
            working_directory=None,
            log=ANY,
            on_dataflow_job_id_found_callback=None,
        )
        mock_virtualenv.assert_called_once_with(
            venv_directory=mock.ANY,
            python_bin="python3",
            system_site_packages=current_py_system_site_packages,
            requirements=current_py_requirements,
        )

    @mock.patch(BEAM_STRING.format("run_beam_command"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.39.0")
    def test_start_python_pipeline_with_empty_py_requirements_and_without_system_packages(
        self, mock_check_output, mock_runner
    ):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        wait_for_done = mock_runner.return_value.wait_for_done
        process_line_callback = MagicMock()
        is_dataflow_job_id_exist_callback = MagicMock()

        with pytest.raises(AirflowException, match=r"Invalid method invocation\."):
            hook.start_python_pipeline(
                variables=copy.deepcopy(BEAM_VARIABLES_PY),
                py_file=PY_FILE,
                py_options=PY_OPTIONS,
                py_requirements=[],
                process_line_callback=process_line_callback,
                is_dataflow_job_id_exist_callback=is_dataflow_job_id_exist_callback,
            )

        mock_runner.assert_not_called()
        wait_for_done.assert_not_called()

    @mock.patch(BEAM_STRING.format("run_beam_command"))
    def test_start_java_pipeline(self, mock_runner):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()

        hook.start_java_pipeline(
            jar=JAR_FILE,
            variables=copy.deepcopy(BEAM_VARIABLES_JAVA),
            process_line_callback=process_line_callback,
        )

        expected_cmd = [
            "java",
            "-jar",
            JAR_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            '--labels={"foo":"bar"}',
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=None,
            log=ANY,
            is_dataflow_job_id_exist_callback=None,
            on_dataflow_job_id_found_callback=None,
        )

    @mock.patch(BEAM_STRING.format("run_beam_command"))
    def test_start_java_pipeline_with_job_class(self, mock_runner):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()

        hook.start_java_pipeline(
            jar=JAR_FILE,
            variables=copy.deepcopy(BEAM_VARIABLES_JAVA),
            job_class=JOB_CLASS,
            process_line_callback=process_line_callback,
        )

        expected_cmd = [
            "java",
            "-cp",
            JAR_FILE,
            JOB_CLASS,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            '--labels={"foo":"bar"}',
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=None,
            log=ANY,
            is_dataflow_job_id_exist_callback=None,
            on_dataflow_job_id_found_callback=None,
        )

    @mock.patch(BEAM_STRING.format("shutil.which"))
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    def test_start_go_pipeline(self, mock_runner, mock_which):
        mock_which.return_value = "/some_path/to/go"
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()

        hook.start_go_pipeline(
            go_file=GO_FILE,
            variables=copy.deepcopy(BEAM_VARIABLES_GO),
            process_line_callback=process_line_callback,
        )

        basename = os.path.basename(GO_FILE)
        go_workspace = os.path.dirname(GO_FILE)
        expected_cmd = [
            "go",
            "run",
            basename,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            '--labels={"foo":"bar"}',
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=go_workspace,
            log=ANY,
            is_dataflow_job_id_exist_callback=None,
            on_dataflow_job_id_found_callback=None,
        )

    @mock.patch(BEAM_STRING.format("shutil.which"))
    def test_start_go_pipeline_without_go_installed_raises(self, mock_which):
        mock_which.return_value = None
        hook = BeamHook(runner=DEFAULT_RUNNER)

        error_message = (
            r"You need to have Go installed to run beam go pipeline\. See .* "
            "installation guide. If you are running airflow in Docker see more info at '.*'"
        )
        with pytest.raises(ConfigException, match=error_message):
            hook.start_go_pipeline(
                go_file=GO_FILE,
                variables=copy.deepcopy(BEAM_VARIABLES_GO),
            )

    @mock.patch(BEAM_STRING.format("run_beam_command"))
    def test_start_go_pipeline_with_binary(self, mock_runner):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()

        launcher_binary = "/path/to/launcher-main"
        worker_binary = "/path/to/worker-main"

        hook.start_go_pipeline_with_binary(
            variables=BEAM_VARIABLES_GO,
            launcher_binary=launcher_binary,
            worker_binary=worker_binary,
            process_line_callback=process_line_callback,
        )

        expected_cmd = [
            launcher_binary,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            '--labels={"foo":"bar"}',
            f"--worker_binary={worker_binary}",
        ]

        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            process_line_callback=process_line_callback,
            working_directory=None,
            log=ANY,
            is_dataflow_job_id_exist_callback=None,
            on_dataflow_job_id_found_callback=None,
        )


class TestBeamRunner:
    @mock.patch("subprocess.Popen")
    @mock.patch("select.select")
    def test_beam_wait_for_done_logging(self, mock_select, mock_popen, caplog):
        logger_name = "fake-beam-wait-for-done-logger"
        fake_logger = logging.getLogger(logger_name)

        cmd = ["fake", "cmd"]
        mock_proc = MagicMock(name="FakeProc")
        fake_stderr_fd = MagicMock(name="FakeStderr")
        fake_stdout_fd = MagicMock(name="FakeStdout")

        mock_proc.stderr = fake_stderr_fd
        mock_proc.stdout = fake_stdout_fd
        fake_stderr_fd.readline.side_effect = [
            b"apache-beam-stderr-1",
            b"apache-beam-stderr-2",
            StopIteration,
            b"apache-beam-stderr-3",
            StopIteration,
            b"apache-beam-other-stderr",
        ]
        fake_stdout_fd.readline.side_effect = [b"apache-beam-stdout", StopIteration]
        mock_select.side_effect = [
            ([fake_stderr_fd], None, None),
            (None, None, None),
            ([fake_stderr_fd], None, None),
        ]
        mock_proc.poll.side_effect = [None, True]
        mock_proc.returncode = 1
        mock_popen.return_value = mock_proc

        caplog.clear()
        with pytest.raises(AirflowException, match="Apache Beam process failed with return code 1"):
            run_beam_command(cmd, fake_logger)

        mock_popen.assert_called_once_with(
            cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, cwd=None
        )
        info_messages = [rt[2] for rt in caplog.record_tuples if rt[0] == logger_name and rt[1] == 20]
        assert "Running command: fake cmd" in info_messages
        assert "apache-beam-stdout" in info_messages

        warn_messages = [rt[2] for rt in caplog.record_tuples if rt[0] == logger_name and rt[1] == 30]
        assert "apache-beam-stderr-1" in warn_messages
        assert "apache-beam-stderr-2" in warn_messages
        assert "apache-beam-stderr-3" in warn_messages
        assert "apache-beam-other-stderr" in warn_messages

    @mock.patch("subprocess.Popen")
    @mock.patch("select.select")
    @mock.patch(
        "airflow.providers.apache.beam.hooks.beam.process_fd"
    )  # Mocking process_fd to control return value
    def test_run_beam_command_with_callbacks(self, mock_process_fd, mock_select, mock_popen):
        """Test that callbacks are passed to process_fd and state is maintained."""
        logger_name = "test-beam-callback-logger"
        fake_logger = logging.getLogger(logger_name)
        cmd = ["fake", "cmd"]

        # Setup mock process
        mock_proc = MagicMock(name="FakeProc")
        mock_proc.stderr = MagicMock(name="FakeStderr")
        mock_proc.stdout = MagicMock(name="FakeStdout")
        mock_proc.poll.side_effect = [None, 0]  # Run loop once, then exit
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        # Mock select to return stdout on first call
        mock_select.return_value = ([mock_proc.stdout], [], [])

        # Callback mocks
        mock_is_exists = MagicMock()
        mock_on_found = MagicMock()

        # We simulate that process_fd returns True (callback was triggered)
        mock_process_fd.return_value = True

        run_beam_command(
            cmd,
            fake_logger,
            is_dataflow_job_id_exist_callback=mock_is_exists,
            on_dataflow_job_id_found_callback=mock_on_found,
        )

        # Verify process_fd was called with the correct arguments
        # Note: It's called inside the loop AND in the "corner case" block at the end
        assert mock_process_fd.call_count >= 2

        # Check that the 'on_dataflow_job_id_found_callback_called'
        # (the 7th positional arg, index 6) was updated correctly between calls
        first_call_args = mock_process_fd.call_args_list[0][0]
        assert first_call_args[6] is False  # Initially False

        last_call_args = mock_process_fd.call_args_list[-1][0]
        assert last_call_args[6] is True  # Became True after mock_process_fd returned True


class TestBeamOptionsToArgs:
    @pytest.mark.parametrize(
        ("options", "expected_args"),
        [
            ({"key": "val"}, ["--key=val"]),
            ({"key": None}, []),
            ({"key": True}, ["--key"]),
            ({"key": False}, []),
            ({"key": ["a", "b", "c"]}, ["--key=a", "--key=b", "--key=c"]),
            ({"key": {"a_key": "a_val", "b_key": "b_val"}}, ['--key={"a_key": "a_val", "b_key": "b_val"}']),
            # Sets False value cases
            ({"use_public_ips": False}, ["--no_use_public_ips"]),
            ({"usePublicIps": False}, ["--usePublicIps=false"]),
        ],
    )
    def test_beam_options_to_args(self, options, expected_args):
        args = beam_options_to_args(options)
        assert args == expected_args


@pytest.fixture
def mocked_beam_version_async():
    with mock.patch.object(BeamAsyncHook, "_beam_version", return_value="2.39.0") as m:
        yield m


class TestBeamAsyncHook:
    @pytest.mark.asyncio
    @pytest.mark.skipif(APACHE_BEAM_VERSION is None, reason="Apache Beam not installed in current env")
    async def test_beam_version(self):
        version = await BeamAsyncHook._beam_version(sys.executable)
        assert version == APACHE_BEAM_VERSION

    @pytest.mark.asyncio
    async def test_beam_version_error(self):
        with pytest.raises(AirflowException, match="Unable to retrieve Apache Beam version"):
            await BeamAsyncHook._beam_version("python1")

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.run_beam_command_async")
    async def test_start_pipline_async(self, mock_runner):
        expected_cmd = [
            *PIPELINE_COMMAND_PREFIX,
            f"--runner={DEFAULT_RUNNER}",
            *beam_options_to_args(BEAM_VARIABLES),
        ]
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)
        await hook.start_pipeline_async(
            variables=BEAM_VARIABLES,
            command_prefix=PIPELINE_COMMAND_PREFIX,
            working_directory=WORKING_DIRECTORY,
        )

        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            working_directory=WORKING_DIRECTORY,
            log=hook.log,
            process_line_callback=None,
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.run_beam_command_async")
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook._create_tmp_dir")
    async def test_start_python_pipeline(self, mock_create_dir, mock_runner, mocked_beam_version_async):
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)
        mock_create_dir.return_value = AsyncMock()
        mock_runner.return_value = 0

        await hook.start_python_pipeline_async(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
        )

        expected_cmd = [
            "python3",
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_create_dir.assert_called_once()
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            working_directory=None,
            log=ANY,
            process_line_callback=None,
        )

    @pytest.mark.asyncio
    async def test_start_python_pipeline_unsupported_option(self, mocked_beam_version_async):
        mocked_beam_version_async.return_value = "2.35.0"
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)

        with pytest.raises(
            AirflowException,
            match=re.escape("The impersonateServiceAccount option requires Apache Beam 2.39.0 or newer."),
        ):
            await hook.start_python_pipeline_async(
                variables={
                    "impersonate_service_account": "test@impersonation.com",
                },
                py_file="/tmp/file.py",
                py_options=["-m"],
                py_interpreter="python3",
                py_requirements=None,
                py_system_site_packages=False,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "py_interpreter",
        [
            pytest.param("python", id="default python"),
            pytest.param("python2", id="major python version 2.x"),
            pytest.param("python3", id="major python version 3.x"),
            pytest.param("python3.6", id="major.minor python version"),
        ],
    )
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.run_beam_command_async")
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook._create_tmp_dir")
    async def test_start_python_pipeline_with_custom_interpreter(
        self,
        mock_create_dir,
        mock_runner,
        py_interpreter,
        mocked_beam_version_async,
    ):
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)
        mock_create_dir.return_value = AsyncMock()
        mock_runner.return_value = 0

        await hook.start_python_pipeline_async(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_interpreter=py_interpreter,
        )

        expected_cmd = [
            py_interpreter,
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            working_directory=None,
            log=ANY,
            process_line_callback=None,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("current_py_requirements", "current_py_system_site_packages"),
        [
            pytest.param("foo-bar", False, id="requirements without system site-packages"),
            pytest.param("foo-bar", True, id="requirements with system site-packages"),
            pytest.param([], True, id="only system site-packages"),
        ],
    )
    @mock.patch(BEAM_STRING.format("prepare_virtualenv"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.run_beam_command_async")
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook._create_tmp_dir")
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook._cleanup_tmp_dir")
    async def test_start_python_pipeline_with_non_empty_py_requirements_and_without_system_packages(
        self,
        mock_cleanup_dir,
        mock_create_dir,
        mock_runner,
        mock_virtualenv,
        current_py_requirements,
        current_py_system_site_packages,
        mocked_beam_version_async,
    ):
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)
        mock_create_dir.return_value = AsyncMock()
        mock_virtualenv.return_value = "/dummy_dir/bin/python"
        mock_cleanup_dir.return_value = AsyncMock()

        await hook.start_python_pipeline_async(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_requirements=current_py_requirements,
            py_system_site_packages=current_py_system_site_packages,
        )

        expected_cmd = [
            "/dummy_dir/bin/python",
            "-m",
            PY_FILE,
            f"--runner={DEFAULT_RUNNER}",
            "--output=gs://test/output",
            "--labels=foo=bar",
        ]
        mock_runner.assert_called_once_with(
            cmd=expected_cmd,
            working_directory=None,
            log=ANY,
            process_line_callback=None,
        )
        mock_virtualenv.assert_called_once_with(
            venv_directory=mock.ANY,
            python_bin="python3",
            system_site_packages=current_py_system_site_packages,
            requirements=current_py_requirements,
        )
        mock_create_dir.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    async def test_start_python_pipeline_with_empty_py_requirements_and_without_system_packages(
        self, mock_runner, mocked_beam_version_async
    ):
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)

        with pytest.raises(AirflowException, match=r"Invalid method invocation\."):
            await hook.start_python_pipeline_async(
                variables=copy.deepcopy(BEAM_VARIABLES_PY),
                py_file=PY_FILE,
                py_options=PY_OPTIONS,
                py_requirements=[],
            )

        mock_runner.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("job_class", "command_prefix"),
        [
            (JOB_CLASS, ["java", "-cp", JAR_FILE, JOB_CLASS]),
            (None, ["java", "-jar", JAR_FILE]),
        ],
    )
    @mock.patch("airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_pipeline_async")
    async def test_start_java_pipeline_async(self, mock_start_pipeline, job_class, command_prefix):
        variables = copy.deepcopy(BEAM_VARIABLES_JAVA)
        hook = BeamAsyncHook(runner=DEFAULT_RUNNER)
        await hook.start_java_pipeline_async(variables=variables, jar=JAR_FILE, job_class=job_class)

        mock_start_pipeline.assert_called_once_with(
            variables=BEAM_VARIABLES_JAVA_STRING_LABELS,
            command_prefix=command_prefix,
            process_line_callback=None,
        )


class TestProcessFD:
    @pytest.fixture
    def mock_proc(self):
        """Fixture to create a mock subprocess with stdout and stderr."""
        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.stderr = MagicMock()
        return proc

    @pytest.fixture
    def mock_log(self):
        """Fixture for the logger."""
        return MagicMock()

    def test_invalid_fd_raises_exception(self, mock_proc, mock_log):
        """Should raise AirflowException if fd is not from the process."""
        invalid_fd = MagicMock()

        with pytest.raises(AirflowException, match="No data in stderr or in stdout."):
            process_fd(mock_proc, invalid_fd, mock_log)

    def test_stdout_logging_flow(self, mock_proc, mock_log):
        """Should read from stdout and log at INFO level."""
        mock_proc.stdout.readline.side_effect = [b"standard output line\n", b""]

        result = process_fd(mock_proc, mock_proc.stdout, mock_log)

        mock_log.info.assert_called_with("standard output line")
        assert result is False

    def test_stderr_logging_flow(self, mock_proc, mock_log):
        """Should read from stderr and log at WARNING level."""
        mock_proc.stderr.readline.side_effect = [b"error line\n", b""]

        process_fd(mock_proc, mock_proc.stderr, mock_log)

        mock_log.warning.assert_called_with("error line")

    def test_callback_triggers_on_job_id_found(self, mock_proc, mock_log):
        """Should execute the callback exactly once when job ID is detected."""
        mock_proc.stdout.readline.side_effect = [b"job log line\n", b""]

        # Mocks for callbacks
        line_cb = MagicMock()
        exists_cb = MagicMock(return_value=True)
        found_cb = MagicMock()

        result = process_fd(
            proc=mock_proc,
            fd=mock_proc.stdout,
            log=mock_log,
            process_line_callback=line_cb,
            is_dataflow_job_id_exist_callback=exists_cb,
            on_dataflow_job_id_found_callback=found_cb,
            on_dataflow_job_id_found_callback_called=False,
        )

        # Assertions
        line_cb.assert_called_with("job log line\n")
        found_cb.assert_called_once()
        mock_log.info.assert_any_call("JOB ID found for dataflow. Calling the callback!")
        assert result is True

    def test_callback_not_called_if_already_flagged(self, mock_proc, mock_log):
        """Should skip callback execution if on_dataflow_job_id_found_callback_called is True."""
        mock_proc.stdout.readline.side_effect = [b"job log line\n", b""]

        exists_cb = MagicMock(return_value=True)
        found_cb = MagicMock()

        result = process_fd(
            proc=mock_proc,
            fd=mock_proc.stdout,
            log=mock_log,
            is_dataflow_job_id_exist_callback=exists_cb,
            on_dataflow_job_id_found_callback=found_cb,
            on_dataflow_job_id_found_callback_called=True,  # Already called
        )

        found_cb.assert_not_called()
        assert result is True

    def test_multi_line_processing(self, mock_proc, mock_log):
        """Ensures all lines are processed until the stream is empty (b"")."""
        mock_proc.stdout.readline.side_effect = [b"line 1\n", b"line 2\n", b""]

        process_fd(mock_proc, mock_proc.stdout, mock_log)

        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls([call("line 1"), call("line 2")])
