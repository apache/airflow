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
import re
import subprocess
from unittest import mock
from unittest.mock import ANY, MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamHook, beam_options_to_args, run_beam_command

PY_FILE = "apache_beam.examples.wordcount"
JAR_FILE = "unitest.jar"
JOB_CLASS = "com.example.UnitTest"
PY_OPTIONS = ["-m"]
TEST_JOB_ID = "test-job-id"

GO_FILE = "/path/to/file.go"
DEFAULT_RUNNER = "DirectRunner"
BEAM_STRING = "airflow.providers.apache.beam.hooks.beam.{}"
BEAM_VARIABLES_PY = {"output": "gs://test/output", "labels": {"foo": "bar"}}
BEAM_VARIABLES_JAVA = {
    "output": "gs://test/output",
    "labels": {"foo": "bar"},
}
BEAM_VARIABLES_GO = {"output": "gs://test/output", "labels": {"foo": "bar"}}

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


class TestBeamHook:
    @mock.patch(BEAM_STRING.format("run_beam_command"))
    @mock.patch("airflow.providers.apache.beam.hooks.beam.subprocess.check_output", return_value=b"2.39.0")
    def test_start_python_pipeline(self, mock_check_output, mock_runner):
        hook = BeamHook(runner=DEFAULT_RUNNER)
        process_line_callback = MagicMock()

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            process_line_callback=process_line_callback,
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

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_interpreter=py_interpreter,
            process_line_callback=process_line_callback,
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
        )

    @pytest.mark.parametrize(
        "current_py_requirements, current_py_system_site_packages",
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

        hook.start_python_pipeline(
            variables=copy.deepcopy(BEAM_VARIABLES_PY),
            py_file=PY_FILE,
            py_options=PY_OPTIONS,
            py_requirements=current_py_requirements,
            py_system_site_packages=current_py_system_site_packages,
            process_line_callback=process_line_callback,
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
            working_directory=None,
            log=ANY,
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

        with pytest.raises(AirflowException, match=r"Invalid method invocation\."):
            hook.start_python_pipeline(
                variables=copy.deepcopy(BEAM_VARIABLES_PY),
                py_file=PY_FILE,
                py_options=PY_OPTIONS,
                py_requirements=[],
                process_line_callback=process_line_callback,
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
            cmd=expected_cmd, process_line_callback=process_line_callback, working_directory=None, log=ANY
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
        )

    @mock.patch(BEAM_STRING.format("shutil.which"))
    def test_start_go_pipeline_without_go_installed_raises(self, mock_which):
        mock_which.return_value = None
        hook = BeamHook(runner=DEFAULT_RUNNER)

        error_message = (
            r"You need to have Go installed to run beam go pipeline\. See .* "
            "installation guide. If you are running airflow in Docker see more info at '.*'"
        )
        with pytest.raises(AirflowException, match=error_message):
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
        )


class TestBeamRunner:
    @mock.patch("subprocess.Popen")
    @mock.patch("select.select")
    def test_beam_wait_for_done_logging(self, mock_select, mock_popen):
        cmd = ["test", "cmd"]
        mock_logging = MagicMock()
        mock_logging.info = MagicMock()
        mock_logging.warning = MagicMock()
        mock_proc = MagicMock()
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readlines = MagicMock(return_value=["test\n", "error\n"])
        mock_stderr_fd = MagicMock()
        mock_proc.stderr.fileno = MagicMock(return_value=mock_stderr_fd)
        mock_proc_poll = MagicMock()
        mock_select.return_value = [[mock_stderr_fd]]

        def poll_resp_error():
            mock_proc.return_code = 1
            return True

        mock_proc_poll.side_effect = [None, poll_resp_error]
        mock_proc.poll = mock_proc_poll
        mock_popen.return_value = mock_proc
        with pytest.raises(Exception):
            run_beam_command(cmd, None, None, mock_logging)
            mock_logging.info.assert_called_once_with("Running command: %s", " ".join(cmd))
            mock_popen.assert_called_once_with(
                cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, cwd=None
            )


class TestBeamOptionsToArgs:
    @pytest.mark.parametrize(
        "options, expected_args",
        [
            ({"key": "val"}, ["--key=val"]),
            ({"key": None}, ["--key"]),
            ({"key": True}, ["--key"]),
            ({"key": False}, ["--key=False"]),
            ({"key": ["a", "b", "c"]}, ["--key=a", "--key=b", "--key=c"]),
        ],
    )
    def test_beam_options_to_args(self, options, expected_args):
        args = beam_options_to_args(options)
        assert args == expected_args
