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

import os
import shutil
from typing import Sequence

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars


class BashOperator(BaseOperator):
    r"""
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the bash command completes

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param append_env: If False(default) uses the environment variables passed in env params
        and does not inherit the current process environment. If True, inherits the environment variables
        from current passes and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it
    :param output_encoding: Output encoding of bash command
    :param skip_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param cwd: Working directory to execute the command in.
        If None (default), the command is run in a temporary directory.

    Airflow will evaluate the exit code of the bash command. In general, a non-zero exit code will result in
    task failure and zero will result in task success. Exit code ``99`` (or another set in ``skip_exit_code``)
    will throw an :class:`airflow.exceptions.AirflowSkipException`, which will leave the task in ``skipped``
    state. You can have all non-zero exit codes be treated as a failure by setting ``skip_exit_code=None``.

    .. list-table::
       :widths: 25 25
       :header-rows: 1

       * - Exit code
         - Behavior
       * - 0
         - success
       * - `skip_exit_code` (default: 99)
         - raise :class:`airflow.exceptions.AirflowSkipException`
       * - otherwise
         - raise :class:`airflow.exceptions.AirflowException`

    .. note::

        Airflow will not recognize a non-zero exit code unless the whole shell exit with a non-zero exit
        code.  This can be an issue if the non-zero exit arises from a sub-command.  The easiest way of
        addressing this is to prefix the command with ``set -e;``

        Example:
        .. code-block:: python

            bash_command = "set -e; python3 script.py '{{ next_execution_date }}'"

    .. note::

        Add a space after the script name when directly calling a ``.sh`` script with the
        ``bash_command`` argument -- for example ``bash_command="my_script.sh "``.  This
        is because Airflow tries to apply load this file and process it as a Jinja template to
        it ends with ``.sh``, which will likely not be what most users want.

    .. warning::

        Care should be taken with "user" input or when using Jinja templates in the
        ``bash_command``, as this bash operator does not perform any escaping or
        sanitization of the command.

        This applies mostly to using "dag_run" conf, as that can be submitted via
        users in the Web UI. Most of the default template variables are not at
        risk.

    For example, do **not** do this:

    .. code-block:: python

        bash_task = BashOperator(
            task_id="bash_task",
            bash_command='echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run else "" }}\'"',
        )

    Instead, you should pass this via the ``env`` kwarg and use double-quotes
    inside the bash_command, as below:

    .. code-block:: python

        bash_task = BashOperator(
            task_id="bash_task",
            bash_command="echo \"here is the message: '$message'\"",
            env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
        )

    """

    template_fields: Sequence[str] = ("bash_command", "env")
    template_fields_renderers = {"bash_command": "bash", "env": "json"}
    template_ext: Sequence[str] = (
        ".sh",
        ".bash",
    )
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        bash_command: str,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        self.skip_exit_code = skip_exit_code
        self.cwd = cwd
        self.append_env = append_env

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command"""
        return SubprocessHook()

    def get_env(self, context):
        """Builds the set of environment variables to be exposed for the bash command"""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        else:
            if self.append_env:
                system_env.update(env)
                env = system_env

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting the following env vars:\n%s",
            "\n".join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def execute(self, context: Context):
        bash_path = shutil.which("bash") or "bash"
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")
        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"Bash command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
            )
        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()
