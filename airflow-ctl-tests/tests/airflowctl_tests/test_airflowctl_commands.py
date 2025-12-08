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

import os
from subprocess import PIPE, STDOUT, Popen

from airflowctl_tests import console


def test_airflowctl_commands(login_command, login_output, test_commands):
    """Test airflowctl commands using docker-compose environment."""
    host_envs = os.environ.copy()
    host_envs["AIRFLOW_CLI_DEBUG_MODE"] = "true"
    # Testing commands of airflowctl
    for command in test_commands:
        command_from_config = f"airflowctl {command}"
        # We need to run auth login first for all commands except login itself
        if command != login_command:
            run_command = f"airflowctl {login_command} && {command_from_config}"
        else:
            run_command = command_from_config
        console.print(f"[yellow]Running command: {command}")

        # Give some time for the command to execute and output to be ready
        proc = Popen(run_command.encode(), stdout=PIPE, stderr=STDOUT, stdin=PIPE, shell=True, env=host_envs)
        stdout_result, stderr_result = proc.communicate(timeout=60)

        # CLI command gave errors
        if stderr_result:
            console.print(
                f"[red]Errors while executing command '{command_from_config}':\n{stderr_result.decode()}"
            )

        # Decode the output
        stdout_result = stdout_result.decode()
        # We need to trim auth login output if the command is not login itself and clean backspaces
        if command != login_command:
            if login_output not in stdout_result:
                console.print(
                    f"[red]❌ Login output not found before command output for '{command_from_config}'"
                )
                console.print(f"[red]Full output:\n{stdout_result}\n")
                raise AssertionError("Login output not found before command output")
            stdout_result = stdout_result.split(f"{login_output}\n")[1].strip()
        else:
            stdout_result = stdout_result.strip()

        # Check for non-zero exit code
        if proc.returncode != 0:
            console.print(f"[red]❌ Command '{command_from_config}' exited with code {proc.returncode}")
            console.print(f"[red]Output:\n{stdout_result}\n")
            raise AssertionError(
                f"Command exited with non-zero code {proc.returncode}\nOutput:\n{stdout_result}"
            )

        # Error patterns to detect failures that might otherwise slip through
        # Please ensure it is aligning with airflowctl.api.client.get_json_error
        error_patterns = [
            "Server error",
            "command error",
            "unrecognized arguments",
            "invalid choice",
            "Traceback (most recent call last):",
        ]
        matched_error = next((error for error in error_patterns if error in stdout_result), None)
        if matched_error:
            console.print(f"[red]❌ Output contained unexpected text for command '{command_from_config}'")
            console.print(f"[red]Matched error pattern: {matched_error}\n")
            console.print(f"[red]Output:\n{stdout_result}\n")
            raise AssertionError(
                f"Output contained error pattern '{matched_error}'\nOutput:\n{stdout_result}"
            )
        console.print(f"[green]✅ Output did not contain unexpected text for command '{command_from_config}'")
        console.print(f"[cyan]Result:\n{stdout_result}\n")
        proc.kill()
