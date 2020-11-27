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
import json
import select
import shlex
import subprocess
import textwrap
from tempfile import TemporaryDirectory
from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.python_virtualenv import prepare_virtualenv


class _BeamRunner(LoggingMixin):
    def __init__(
        self,
        cmd: List[str],
    ) -> None:
        super().__init__()
        self.log.info("Running command: %s", " ".join(shlex.quote(c) for c in cmd))
        self._proc = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )

    def _process_fd(self, fd):
        """
        Prints output to logs.

        :param fd: File descriptor.
        """
        if fd == self._proc.stderr:
            while True:
                line = self._proc.stderr.readline().decode()
                if not line:
                    return
                self.log.warning(line.rstrip("\n"))

        if fd == self._proc.stdout:
            while True:
                line = self._proc.stdout.readline().decode()
                if not line:
                    return
                self.log.info(line.rstrip("\n"))

        raise Exception("No data in stderr or in stdout.")

    def wait_for_done(self) -> None:
        """Waits for Apache Beam pipeline to complete."""
        self.log.info("Start waiting for Apache Beam process to complete.")
        reads = [self._proc.stderr, self._proc.stdout]
        while True:
            # Wait for at least one available fd.
            readable_fds, _, _ = select.select(reads, [], [], 5)
            if readable_fds is None:
                self.log.info("Waiting for Apache Beam process to complete.")
                continue

            for readable_fd in readable_fds:
                self._process_fd(readable_fd)

            if self._proc.poll() is not None:
                break

        # Corner case: check if more output was created between the last read and the process termination
        for readable_fd in reads:
            self._process_fd(readable_fd)

        self.log.info("Process exited with return code: %s", self._proc.returncode)

        if self._proc.returncode != 0:
            raise Exception(f"Apache Beam process failed with return code {self._proc.returncode}")


class BeamHook(BaseHook):
    """
    Hook for Apache Beam.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
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
        command_prefix: List[str],
    ) -> None:
        cmd = command_prefix + [
            f"--runner={self.runner}",
        ]
        if variables:
            cmd.extend(self._options_to_args(variables))
        _BeamRunner(cmd=cmd).wait_for_done()

    @staticmethod
    def _options_to_args(variables: dict) -> List[str]:
        if not variables:
            return []
        # The logic of this method should be compatible with Apache Beam:
        # https://github.com/apache/beam/blob/b56740f0e8cd80c2873412847d0b336837429fb9/sdks/python/
        # apache_beam/options/pipeline_options.py#L230-L251
        args: List[str] = []
        for attr, value in variables.items():
            if value is None or (isinstance(value, bool) and value):
                args.append(f"--{attr}")
            elif isinstance(value, list):
                args.extend([f"--{attr}={v}" for v in value])
            else:
                args.append(f"--{attr}={value}")
        return args

    def start_python_pipeline(  # pylint: disable=too-many-arguments
        self,
        variables: dict,
        py_file: str,
        py_options: List[str],
        py_interpreter: str = "python3",
        py_requirements: Optional[List[str]] = None,
        py_system_site_packages: bool = False,
    ):
        """
        Starts Apache Beam python pipeline.

        :param variables: Variables passed to the pipeline.
        :type variables: Dict
        :param py_options: Additional options.
        :type py_options: List[str]
        :param py_interpreter: Python version of the Apache Beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.

            You could also install the apache-beam package if it is not installed on your system or you want
            to use a different version.
        :type py_requirements: List[str]
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.

            This option is only relevant if the ``py_requirements`` parameter is not None.
        :type py_interpreter: str
        """
        if "labels" in variables:
            variables["labels"] = [f"{key}={value}" for key, value in variables["labels"].items()]

        if py_requirements is not None:
            if not py_requirements and not py_system_site_packages:
                warning_invalid_environment = textwrap.dedent(
                    """\
                    Invalid method invocation. You have disabled inclusion of system packages and empty list
                    required for installation, so it is not possible to create a valid virtual environment.
                    In the virtual environment, apache-beam package must be installed for your job to be \
                    executed. To fix this problem:
                    * install apache-beam on the system, then set parameter py_system_site_packages to True,
                    * add apache-beam to the list of required packages in parameter py_requirements.
                    """
                )
                raise AirflowException(warning_invalid_environment)

            with TemporaryDirectory(prefix="apache-beam-venv") as tmp_dir:
                py_interpreter = prepare_virtualenv(
                    venv_directory=tmp_dir,
                    python_bin=py_interpreter,
                    system_site_packages=py_system_site_packages,
                    requirements=py_requirements,
                )
                command_prefix = [py_interpreter] + py_options + [py_file]

                self._start_pipeline(
                    variables=variables,
                    command_prefix=command_prefix,
                )
        else:
            command_prefix = [py_interpreter] + py_options + [py_file]

            self._start_pipeline(
                variables=variables,
                command_prefix=command_prefix,
            )

    def start_java_pipeline(
        self,
        variables: dict,
        jar: str,
        job_class: Optional[str] = None,
    ) -> None:
        """
        Starts Apache Beam Java pipeline.

        :param variables: Variables passed to the job.
        :type variables: dict
        :param jar: Name of the jar for the pipeline
        :type job_class: str
        :param job_class: Name of the java class for the pipeline.
        :type job_class: str
        """
        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        command_prefix = ["java", "-cp", jar, job_class] if job_class else ["java", "-jar", jar]
        self._start_pipeline(
            variables=variables,
            command_prefix=command_prefix,
        )
