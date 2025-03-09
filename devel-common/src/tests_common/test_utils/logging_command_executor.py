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
from __future__ import annotations

import shlex
import subprocess

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class CommandExecutionError(Exception):
    """Raise in case of error during command execution."""


class LoggingCommandExecutor(LoggingMixin):
    """Logging command executor."""

    def execute_cmd(self, cmd, silent=False, cwd=None, env=None):
        if silent:
            self.log.info("Executing in silent mode: '%s'", " ".join(shlex.quote(c) for c in cmd))
            return subprocess.call(
                args=cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env, cwd=cwd
            )
        else:
            self.log.info("Executing: '%s'", " ".join(shlex.quote(c) for c in cmd))
            with subprocess.Popen(
                args=cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                cwd=cwd,
                env=env,
            ) as process:
                output, err = process.communicate()
                retcode = process.poll()
                self.log.info("Stdout: %s", output)
                self.log.info("Stderr: %s", err)
                if retcode:
                    self.log.error("Error when executing %s", " ".join(shlex.quote(c) for c in cmd))
                return retcode

    def check_output(self, cmd):
        self.log.info("Executing for output: '%s'", " ".join(shlex.quote(c) for c in cmd))
        with subprocess.Popen(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            output, err = process.communicate()
            retcode = process.poll()
            if retcode:
                self.log.error("Error when executing '%s'", " ".join(shlex.quote(c) for c in cmd))
                self.log.info("Stdout: %s", output)
                self.log.info("Stderr: %s", err)
                raise AirflowException(
                    f"Retcode {retcode} on {' '.join(cmd)} with stdout: {output}, stderr: {err}"
                )
            return output


class CommandExecutor(LoggingCommandExecutor):
    """
    Command executor.

    Due to 'LoggingCommandExecutor' class just returns the status code of command execution
    ('execute_cmd' method) and continues to perform code with possible errors, separate
    inherited 'CommandExecutor' class was created to use it if you need to break code performing
    and immediately raise an exception in case of error during command execution,
    so re-written 'execute_cmd' method will raise 'CommandExecutionError' exception.
    """

    def execute_cmd(self, cmd, silent=False, cwd=None, env=None):
        if silent:
            self.log.info("Executing in silent mode: '%s'", " ".join(shlex.quote(c) for c in cmd))
            return subprocess.call(
                args=cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env, cwd=cwd
            )
        else:
            self.log.info("Executing: '%s'", " ".join(shlex.quote(c) for c in cmd))
            with subprocess.Popen(
                args=cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                cwd=cwd,
                env=env,
            ) as process:
                output, err = process.communicate()
                retcode = process.poll()
                if retcode:
                    raise CommandExecutionError(
                        f"Error when executing '{' '.join(cmd)}' with stdout: {output}, stderr: {err}"
                    )
                self.log.info("Stdout: %s", output)
                self.log.info("Stderr: %s", err)


def get_executor() -> LoggingCommandExecutor:
    return LoggingCommandExecutor()
