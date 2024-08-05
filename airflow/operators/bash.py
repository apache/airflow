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
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Container, Sequence, cast

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.types import ArgNotSet

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.context import Context


class BashOperator(BaseOperator):
    r"""
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the bash command completes

    :param bash_command: The command, set of commands or reference to a
        Bash script (must be '.sh' or '.bash') to be executed. (templated)
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param append_env: If False(default) uses the environment variables passed in env params
        and does not inherit the current process environment. If True, inherits the environment variables
        from current passes and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it
    :param output_encoding: Output encoding of Bash command
    :param skip_on_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param cwd: Working directory to execute the command in (templated).
        If None (default), the command is run in a temporary directory.
        To use current DAG folder as the working directory,
        you might set template ``{{ dag_run.dag.folder }}``.
    :param output_processor: Function to further process the output of the bash script
        (default is lambda output: output).

    Airflow will evaluate the exit code of the Bash command. In general, a non-zero exit code will result in
    task failure and zero will result in task success.
    Exit code ``99`` (or another set in ``skip_on_exit_code``)
    will throw an :class:`airflow.exceptions.AirflowSkipException`, which will leave the task in ``skipped``
    state. You can have all non-zero exit codes be treated as a failure by setting ``skip_on_exit_code=None``.

    .. list-table::
       :widths: 25 25
       :header-rows: 1

       * - Exit code
         - Behavior
       * - 0
         - success
       * - `skip_on_exit_code` (default: 99)
         - raise :class:`airflow.exceptions.AirflowSkipException`
       * - otherwise
         - raise :class:`airflow.exceptions.AirflowException`

    .. note::

        Airflow will not recognize a non-zero exit code unless the whole shell exit with a non-zero exit
        code.  This can be an issue if the non-zero exit arises from a sub-command.  The easiest way of
        addressing this is to prefix the command with ``set -e;``

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

    .. versionadded:: 2.10.0
       The `output_processor` parameter.

    """

    template_fields: Sequence[str] = ("bash_command", "env", "cwd")
    template_fields_renderers = {"bash_command": "bash", "env": "json"}
    template_ext: Sequence[str] = (".sh", ".bash")
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        bash_command: str | ArgNotSet,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        output_processor: Callable[[str], Any] = lambda result: result,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        if skip_exit_code is not None:
            warnings.warn(
                "skip_exit_code is deprecated. Please use skip_on_exit_code", DeprecationWarning, stacklevel=2
            )
            skip_on_exit_code = skip_exit_code
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
            else []
        )
        self.cwd = cwd
        self.append_env = append_env
        self.output_processor = output_processor

        # When using the @task.bash decorator, the Bash command is not known until the underlying Python
        # callable is executed and therefore set to NOTSET initially. This flag is useful during execution to
        # determine whether the bash_command value needs to re-rendered.
        self._init_bash_command_not_set = isinstance(self.bash_command, ArgNotSet)

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    @staticmethod
    def refresh_bash_command(ti: TaskInstance) -> None:
        """
        Rewrite the underlying rendered bash_command value for a task instance in the metadatabase.

        TaskInstance.get_rendered_template_fields() cannot be used because this will retrieve the
        RenderedTaskInstanceFields from the metadatabase which doesn't have the runtime-evaluated bash_command
        value.

        :meta private:
        """
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        RenderedTaskInstanceFields._update_runtime_evaluated_template_fields(ti)

    def get_env(self, context):
        """Build the set of environment variables to be exposed for the bash command."""
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
            "Exporting env vars: %s",
            " ".join(f"{k}={v!r}" for k, v in airflow_context_vars.items()),
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

        # Because the bash_command value is evaluated at runtime using the @task.bash decorator, the
        # RenderedTaskInstanceField data needs to be rewritten and the bash_command value re-rendered -- the
        # latter because the returned command from the decorated callable could contain a Jinja expression.
        # Both will ensure the correct Bash command is executed and that the Rendered Template view in the UI
        # displays the executed command (otherwise it will display as an ArgNotSet type).
        if self._init_bash_command_not_set:
            ti = cast("TaskInstance", context["ti"])
            self.refresh_bash_command(ti)

        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )
        if result.exit_code in self.skip_on_exit_code:
            raise AirflowSkipException(f"Bash command returned exit code {result.exit_code}. Skipping.")
        elif result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
            )

        return self.output_processor(result.output)

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()
