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
"""This module contains a Apache Beam Hook."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import functools
import json
import os
import select
import shlex
import shutil
import subprocess
import tempfile
import textwrap
from typing import TYPE_CHECKING, Callable

from packaging.version import Version

from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.google.go_module_utils import init_module, install_dependencies
from airflow.utils.python_virtualenv import prepare_virtualenv

if TYPE_CHECKING:
    import logging

_APACHE_BEAM_VERSION_SCRIPT = "import apache_beam; print(apache_beam.__version__)"


class BeamRunnerType:
    """
    Helper class for listing runner types.

    For more information about runners see: https://beam.apache.org/documentation/
    """

    DataflowRunner = "DataflowRunner"
    DirectRunner = "DirectRunner"
    SparkRunner = "SparkRunner"
    FlinkRunner = "FlinkRunner"
    SamzaRunner = "SamzaRunner"
    NemoRunner = "NemoRunner"
    JetRunner = "JetRunner"
    Twister2Runner = "Twister2Runner"


def beam_options_to_args(options: dict) -> list[str]:
    """
    Return a formatted pipeline options from a dictionary of arguments.

    The logic of this method should be compatible with Apache Beam:
    https://github.com/apache/beam/blob/b56740f0e8cd80c2873412847d0b336837429fb9/sdks/python/
    apache_beam/options/pipeline_options.py#L230-L251

    :param options: Dictionary with options
    :return: List of arguments
    """
    if not options:
        return []

    args: list[str] = []
    for attr, value in options.items():
        if value is None or (isinstance(value, bool) and value):
            args.append(f"--{attr}")
        elif isinstance(value, bool) and not value:
            continue
        elif isinstance(value, list):
            args.extend([f"--{attr}={v}" for v in value])
        else:
            args.append(f"--{attr}={value}")
    return args


def process_fd(
    proc,
    fd,
    log: logging.Logger,
    process_line_callback: Callable[[str], None] | None = None,
    check_job_status_callback: Callable[[], bool | None] | None = None,
):
    """
    Print output to logs.

    :param proc: subprocess.
    :param fd: File descriptor.
    :param process_line_callback: Optional callback which can be used to process
        stdout and stderr to detect job id.
    :param log: logger.
    """
    if fd not in (proc.stdout, proc.stderr):
        raise AirflowException("No data in stderr or in stdout.")

    fd_to_log = {proc.stderr: log.warning, proc.stdout: log.info}
    func_log = fd_to_log[fd]

    for line in iter(fd.readline, b""):
        line = line.decode()
        if process_line_callback:
            process_line_callback(line)
        func_log(line.rstrip("\n"))
        if check_job_status_callback and check_job_status_callback():
            return


def run_beam_command(
    cmd: list[str],
    log: logging.Logger,
    process_line_callback: Callable[[str], None] | None = None,
    working_directory: str | None = None,
    check_job_status_callback: Callable[[], bool | None] | None = None,
) -> None:
    """
    Run pipeline command in subprocess.

    :param cmd: Parts of the command to be run in subprocess
    :param process_line_callback: Optional callback which can be used to process
        stdout and stderr to detect job id
    :param working_directory: Working directory
    :param log: logger.
    """
    log.info("Running command: %s", " ".join(shlex.quote(c) for c in cmd))

    proc = subprocess.Popen(
        cmd,
        cwd=working_directory,
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    # Waits for Apache Beam pipeline to complete.
    log.info("Start waiting for Apache Beam process to complete.")
    reads = [proc.stderr, proc.stdout]
    while True:
        # Wait for at least one available fd.
        readable_fds, _, _ = select.select(reads, [], [], 5)
        if readable_fds is None:
            log.info("Waiting for Apache Beam process to complete.")
            continue

        for readable_fd in readable_fds:
            process_fd(
                proc, readable_fd, log, process_line_callback, check_job_status_callback
            )
            if check_job_status_callback and check_job_status_callback():
                return

        if proc.poll() is not None:
            break

    # Corner case: check if more output was created between the last read and the process termination
    for readable_fd in reads:
        process_fd(
            proc, readable_fd, log, process_line_callback, check_job_status_callback
        )

    log.info("Process exited with return code: %s", proc.returncode)

    if proc.returncode != 0:
        raise AirflowException(
            f"Apache Beam process failed with return code {proc.returncode}"
        )


class BeamHook(BaseHook):
    """
    Hook for Apache Beam.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param runner: Runner type
    """

    def __init__(
        self,
        runner: str,
    ) -> None:
        self.runner = runner
        super().__init__()

    def _start_pipeline(
        self,
        variables: dict,
        command_prefix: list[str],
        process_line_callback: Callable[[str], None] | None = None,
        working_directory: str | None = None,
        check_job_status_callback: Callable[[], bool | None] | None = None,
    ) -> None:
        cmd = [*command_prefix, f"--runner={self.runner}"]
        if variables:
            cmd.extend(beam_options_to_args(variables))
        run_beam_command(
            cmd=cmd,
            process_line_callback=process_line_callback,
            working_directory=working_directory,
            log=self.log,
            check_job_status_callback=check_job_status_callback,
        )

    def start_python_pipeline(
        self,
        variables: dict,
        py_file: str,
        py_options: list[str],
        py_interpreter: str = "python3",
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        process_line_callback: Callable[[str], None] | None = None,
        check_job_status_callback: Callable[[], bool | None] | None = None,
    ):
        """
        Start Apache Beam python pipeline.

        :param variables: Variables passed to the pipeline.
        :param py_file: Path to the python file to execute.
        :param py_options: Additional options.
        :param py_interpreter: Python version of the Apache Beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.

            You could also install the apache-beam package if it is not installed on your system, or you want
            to use a different version.
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.

            This option is only relevant if the ``py_requirements`` parameter is not None.
        :param process_line_callback: (optional) Callback that can be used to process each line of
            the stdout and stderr file descriptors.
        """
        if "labels" in variables:
            variables["labels"] = [
                f"{key}={value}" for key, value in variables["labels"].items()
            ]

        with contextlib.ExitStack() as exit_stack:
            if py_requirements is not None:
                if not py_requirements and not py_system_site_packages:
                    warning_invalid_environment = textwrap.dedent(
                        """\
                        Invalid method invocation. You have disabled inclusion of system packages and empty
                        list required for installation, so it is not possible to create a valid virtual
                        environment. In the virtual environment, apache-beam package must be installed for
                        your job to be executed.

                        To fix this problem:
                        * install apache-beam on the system, then set parameter py_system_site_packages
                          to True,
                        * add apache-beam to the list of required packages in parameter py_requirements.
                        """
                    )
                    raise AirflowException(warning_invalid_environment)
                tmp_dir = exit_stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="apache-beam-venv")
                )
                py_interpreter = prepare_virtualenv(
                    venv_directory=tmp_dir,
                    python_bin=py_interpreter,
                    system_site_packages=py_system_site_packages,
                    requirements=py_requirements,
                )

            command_prefix = [py_interpreter, *py_options, py_file]

            beam_version = (
                subprocess.check_output(
                    [py_interpreter, "-c", _APACHE_BEAM_VERSION_SCRIPT]
                )
                .decode()
                .strip()
            )
            self.log.info("Beam version: %s", beam_version)
            impersonate_service_account = variables.get("impersonate_service_account")
            if impersonate_service_account:
                if Version(beam_version) < Version("2.39.0"):
                    raise AirflowException(
                        "The impersonateServiceAccount option requires Apache Beam 2.39.0 or newer."
                    )
            self._start_pipeline(
                variables=variables,
                command_prefix=command_prefix,
                process_line_callback=process_line_callback,
                check_job_status_callback=check_job_status_callback,
            )

    def start_java_pipeline(
        self,
        variables: dict,
        jar: str,
        job_class: str | None = None,
        process_line_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Start Apache Beam Java pipeline.

        :param variables: Variables passed to the job.
        :param jar: Name of the jar for the pipeline
        :param job_class: Name of the java class for the pipeline.
        :param process_line_callback: (optional) Callback that can be used to process each line of
            the stdout and stderr file descriptors.
        """
        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        command_prefix = (
            ["java", "-cp", jar, job_class] if job_class else ["java", "-jar", jar]
        )
        self._start_pipeline(
            variables=variables,
            command_prefix=command_prefix,
            process_line_callback=process_line_callback,
        )

    def start_go_pipeline(
        self,
        variables: dict,
        go_file: str,
        process_line_callback: Callable[[str], None] | None = None,
        should_init_module: bool = False,
    ) -> None:
        """
        Start Apache Beam Go pipeline with a source file.

        :param variables: Variables passed to the job.
        :param go_file: Path to the Go file with your beam pipeline.
        :param process_line_callback: (optional) Callback that can be used to process each line of
            the stdout and stderr file descriptors.
        :param should_init_module: If False (default), will just execute a `go run` command. If True, will
            init a module and dependencies with a ``go mod init`` and ``go mod tidy``, useful when pulling
            source with GCSHook.
        :return:
        """
        if shutil.which("go") is None:
            raise AirflowConfigException(
                "You need to have Go installed to run beam go pipeline. See https://go.dev/doc/install "
                "installation guide. If you are running airflow in Docker see more info at "
                "'https://airflow.apache.org/docs/docker-stack/recipes.html'."
            )

        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        working_directory = os.path.dirname(go_file)
        basename = os.path.basename(go_file)

        if should_init_module:
            init_module("main", working_directory)
            install_dependencies(working_directory)

        command_prefix = ["go", "run", basename]
        self._start_pipeline(
            variables=variables,
            command_prefix=command_prefix,
            process_line_callback=process_line_callback,
            working_directory=working_directory,
        )

    def start_go_pipeline_with_binary(
        self,
        variables: dict,
        launcher_binary: str,
        worker_binary: str,
        process_line_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Start Apache Beam Go pipeline with an executable binary.

        :param variables: Variables passed to the job.
        :param launcher_binary: Path to the binary compiled for the launching platform.
        :param worker_binary: Path to the binary compiled for the worker platform.
        :param process_line_callback: (optional) Callback that can be used to process each line of
            the stdout and stderr file descriptors.
        """
        job_variables = copy.deepcopy(variables)

        if "labels" in job_variables:
            job_variables["labels"] = json.dumps(
                job_variables["labels"], separators=(",", ":")
            )

        job_variables["worker_binary"] = worker_binary

        command_prefix = [launcher_binary]

        self._start_pipeline(
            variables=job_variables,
            command_prefix=command_prefix,
            process_line_callback=process_line_callback,
        )


class BeamAsyncHook(BeamHook):
    """
    Asynchronous hook for Apache Beam.

    :param runner: Runner type.
    """

    def __init__(
        self,
        runner: str,
    ) -> None:
        self.runner = runner
        super().__init__(runner=self.runner)

    @staticmethod
    async def _create_tmp_dir(prefix: str) -> str:
        """Create temporary directory."""
        # Creating separate thread to create temporary directory
        loop = asyncio.get_running_loop()
        partial_func = functools.partial(tempfile.mkdtemp, prefix=prefix)
        tmp_dir = await loop.run_in_executor(None, partial_func)
        return tmp_dir

    @staticmethod
    async def _cleanup_tmp_dir(tmp_dir: str) -> None:
        """
        Delete temporary directory after finishing work with it.

        Is uses `rmtree` method to recursively remove the temporary directory.
        """
        shutil.rmtree(tmp_dir)

    @staticmethod
    async def _beam_version(py_interpreter: str) -> str:
        version_script_cmd = shlex.join(
            [py_interpreter, "-c", _APACHE_BEAM_VERSION_SCRIPT]
        )
        proc = await asyncio.create_subprocess_shell(
            version_script_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            msg = (
                f"Unable to retrieve Apache Beam version, return code {proc.returncode}."
                f"\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
            )
            raise AirflowException(msg)
        return stdout.decode().strip()

    async def start_python_pipeline_async(
        self,
        variables: dict,
        py_file: str,
        py_options: list[str] | None = None,
        py_interpreter: str = "python3",
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
    ):
        """
        Start Apache Beam python pipeline.

        :param variables: Variables passed to the pipeline.
        :param py_file: Path to the python file to execute.
        :param py_options: Additional options.
        :param py_interpreter: Python version of the Apache Beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.
            You could also install the apache-beam package if it is not installed on your system, or you want
            to use a different version.
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.
            This option is only relevant if the ``py_requirements`` parameter is not None.
        """
        py_options = py_options or []
        if "labels" in variables:
            variables["labels"] = [
                f"{key}={value}" for key, value in variables["labels"].items()
            ]

        # Creating temporary directory
        tmp_dir = await self._create_tmp_dir(prefix="apache-beam-venv")

        async with contextlib.AsyncExitStack() as exit_stack:
            if py_requirements is not None:
                if not py_requirements and not py_system_site_packages:
                    warning_invalid_environment = textwrap.dedent(
                        """\
                        Invalid method invocation. You have disabled inclusion of system packages and empty
                        list required for installation, so it is not possible to create a valid virtual
                        environment. In the virtual environment, apache-beam package must be installed for
                        your job to be executed.

                        To fix this problem:
                        * install apache-beam on the system, then set parameter py_system_site_packages
                          to True,
                        * add apache-beam to the list of required packages in parameter py_requirements.
                        """
                    )
                    raise AirflowException(warning_invalid_environment)

                # Pushing asynchronous callback to ensure the cleanup of the temporary
                # directory when the asynchronous context is exited
                exit_stack.push_async_callback(self._cleanup_tmp_dir, tmp_dir)

                py_interpreter = prepare_virtualenv(
                    venv_directory=tmp_dir,
                    python_bin=py_interpreter,
                    system_site_packages=py_system_site_packages,
                    requirements=py_requirements,
                )
            command_prefix: list[str] = [py_interpreter, *py_options, py_file]
            beam_version = await self._beam_version(py_interpreter)
            self.log.info("Beam version: %s", beam_version)
            impersonate_service_account = variables.get("impersonate_service_account")
            if impersonate_service_account:
                if Version(beam_version) < Version("2.39.0"):
                    raise AirflowException(
                        "The impersonateServiceAccount option requires Apache Beam 2.39.0 or newer."
                    )
            return_code = await self.start_pipeline_async(
                variables=variables,
                command_prefix=command_prefix,
            )
            return return_code

    async def start_java_pipeline_async(
        self, variables: dict, jar: str, job_class: str | None = None
    ):
        """
        Start Apache Beam Java pipeline.

        :param variables: Variables passed to the job.
        :param jar: Name of the jar for the pipeline.
        :param job_class: Name of the java class for the pipeline.
        :return: Beam command execution return code.
        """
        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        command_prefix = (
            ["java", "-cp", jar, job_class] if job_class else ["java", "-jar", jar]
        )
        return_code = await self.start_pipeline_async(
            variables=variables,
            command_prefix=command_prefix,
        )
        return return_code

    async def start_pipeline_async(
        self,
        variables: dict,
        command_prefix: list[str],
        working_directory: str | None = None,
    ) -> int:
        cmd = [*command_prefix, f"--runner={self.runner}"]
        if variables:
            cmd.extend(beam_options_to_args(variables))
        return await self.run_beam_command_async(
            cmd=cmd,
            working_directory=working_directory,
            log=self.log,
        )

    async def run_beam_command_async(
        self,
        cmd: list[str],
        log: logging.Logger,
        working_directory: str | None = None,
    ) -> int:
        """
        Run pipeline command in subprocess.

        :param cmd: Parts of the command to be run in subprocess
        :param working_directory: Working directory
        :param log: logger.
        """
        cmd_str_representation = " ".join(shlex.quote(c) for c in cmd)
        log.info("Running command: %s", cmd_str_representation)

        # Creating a separate asynchronous process
        process = await asyncio.create_subprocess_shell(
            cmd_str_representation,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            cwd=working_directory,
        )
        # Waits for Apache Beam pipeline to complete.
        log.info("Start waiting for Apache Beam process to complete.")

        # Creating separate threads for stdout and stderr
        stdout_task = asyncio.create_task(self.read_logs(process.stdout))
        stderr_task = asyncio.create_task(self.read_logs(process.stderr))

        # Waiting for the both tasks to complete
        await asyncio.gather(stdout_task, stderr_task)

        # Wait for the process to complete and return return_code
        return_code = await process.wait()
        log.info("Process exited with return code: %s", return_code)

        if return_code != 0:
            raise AirflowException(
                f"Apache Beam process failed with return code {return_code}"
            )
        return return_code

    async def read_logs(self, stream_reader):
        while True:
            line = await stream_reader.readline()
            if not line:
                break
            decoded_line = line.decode().strip()
            self.log.info(decoded_line)
