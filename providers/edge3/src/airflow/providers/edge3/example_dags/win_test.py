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
"""
In this DAG some tests are made to check a worker on Windows.

The DAG is created in conjunction with the documentation in
https://github.com/apache/airflow/blob/main/providers/edge3/docs/install_on_windows.rst
and serves as a PoC test for the Windows worker.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Container, Sequence
from datetime import datetime
from subprocess import STDOUT, Popen
from time import sleep
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.providers.common.compat.sdk import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.execution_time.context import context_to_airflow_vars

try:
    from airflow.sdk import task, task_group
except ImportError:
    from airflow.decorators import task, task_group  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models import Param  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
try:
    from airflow.providers.common.compat.standard.operators import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]
try:
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
except ImportError:
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk.definitions._internal.types import is_arg_set
except ImportError:

    def is_arg_set(value):  # type: ignore[misc,no-redef]
        return value is not NOTSET


if TYPE_CHECKING:
    from airflow.sdk import Context
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as TaskInstance


class CmdOperator(BaseOperator):
    r"""Execute a command or batch of commands.

    This operator is forked of BashOperator to execute any process on windows.

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the command completes

    :param command: The command, set of commands or reference to a
        BAT script (must be '.bat') to be executed. (templated)
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param append_env: If False(default) uses the environment variables passed in env params
        and does not inherit the current process environment. If True, inherits the environment variables
        from current passes and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it
    :param skip_on_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param cwd: Working directory to execute the command in (templated).
        If None (default), the command is run in a temporary directory.
        To use current DAG folder as the working directory,
        you might set template ``{{ task.dag.folder }}``.
    :param output_processor: Function to further process the output of the script / command
        (default is lambda output: output).

    Airflow will evaluate the exit code of the command. In general, a non-zero exit code will result in
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

    .. warning::

        Care should be taken with "user" input or when using Jinja templates in the
        ``command``, as this command operator does not perform any escaping or
        sanitization of the command.

        This applies mostly to using "dag_run" conf, as that can be submitted via
        users in the Web UI. Most of the default template variables are not at
        risk.

    """

    template_fields: Sequence[str] = ("command", "env", "cwd")
    template_fields_renderers = {"command": "bash", "env": "json"}
    template_ext: Sequence[str] = ".bat"

    subprocess: Popen | None = None

    def __init__(
        self,
        *,
        command: list[str] | str | ArgNotSet,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        output_processor: Callable[[str], Any] = lambda result: result,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command.split(" ") if isinstance(command, str) else command
        self.env = env
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

        # When using the @task.command decorator, the command is not known until the underlying Python
        # callable is executed and therefore set to NOTSET initially. This flag is useful during execution to
        # determine whether the command value needs to re-rendered.
        self._init_command_not_set = not is_arg_set(self.command)

    @staticmethod
    def refresh_command(ti: TaskInstance) -> None:
        """Rewrite the underlying rendered command value for a task instance in the metadatabase.

        TaskInstance.get_rendered_template_fields() cannot be used because this will retrieve the
        RenderedTaskInstanceFields from the metadatabase which doesn't have the runtime-evaluated command
        value.

        :meta private:
        """
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        RenderedTaskInstanceFields._update_runtime_evaluated_template_fields(ti)

    def get_env(self, context):
        """Build the set of environment variables to be exposed for the command."""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        elif self.append_env:
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
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")
        env = self.get_env(context)

        # Because the command value is evaluated at runtime using the @task.command decorator, the
        # RenderedTaskInstanceField data needs to be rewritten and the command value re-rendered -- the
        # latter because the returned command from the decorated callable could contain a Jinja expression.
        # Both will ensure the correct command is executed and that the Rendered Template view in the UI
        # displays the executed command (otherwise it will display as an ArgNotSet type).
        if self._init_command_not_set:
            ti = context["ti"]
            self.refresh_command(ti)

        self.subprocess = Popen(
            args=self.command,  # type: ignore # here we assume the arg has been replaced by a string array
            shell=True,
            env=env,
            stderr=STDOUT,
            text=True,
            cwd=self.cwd,
        )
        outs, _ = self.subprocess.communicate()
        exit_code = self.subprocess.returncode
        if exit_code in self.skip_on_exit_code:
            raise AirflowSkipException(f"Command returned exit code {exit_code}. Skipping.")
        if exit_code != 0:
            raise AirflowException(f"Command failed. The command returned a non-zero exit code {exit_code}.")

        return self.output_processor(outs)

    def on_kill(self) -> None:
        if self.subprocess:
            self.subprocess.kill()


with DAG(
    dag_id="win_test",
    dag_display_name="Windows Test",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    tags=["edge", "Windows"],
    default_args={"queue": "windows"},
    params={
        "mapping_count": Param(
            4,
            type="integer",
            title="Mapping Count",
            description="Amount of tasks that should be mapped",
        ),
    },
) as dag:

    @task
    def my_setup():
        print("Assume this is a setup task")

    @task
    def mapping_from_params(**context) -> list[int]:
        mapping_count: int = context["params"]["mapping_count"]
        return list(range(1, mapping_count + 1))

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    @task_group(prefix_group_id=False)
    def mapping_task_group():
        added_values = add_one.expand(x=mapping_from_params())
        sum_it(added_values)

    @task_group(prefix_group_id=False)
    def standard_tasks_group():
        @task.branch
        def branching():
            return ["virtualenv", "variable", "connection", "command", "classic_python"]

        @task.virtualenv(requirements="numpy")
        def virtualenv():
            import numpy

            print(f"Welcome to virtualenv with numpy version {numpy.__version__}.")

        @task
        def variable():
            print("Creating a new variable...")
            Variable.set("integration_test_key", "value")
            print(f"For the moment the variable is set to {Variable.get('integration_test_key')}")
            print("Deleting variable...")
            Variable.delete("integration_test_key")

        @task
        def connection():
            try:
                conn = BaseHook.get_connection("integration_test")
                print(f"Got connection {conn}")
            except AirflowNotFoundException:
                print("Connection 'integration_test' not found... but also OK.")

        command = CmdOperator(task_id="command", command="echo Hello World")

        def python_call():
            print("Hello world")

        classic_py = PythonOperator(task_id="classic_python", python_callable=python_call)

        empty = EmptyOperator(task_id="not_executed")

        branching() >> [virtualenv(), variable(), connection(), command, classic_py, empty]

    @task_group(prefix_group_id=False)
    def failure_tests_group():
        @task
        def plan_to_fail():
            print("This task is supposed to fail")
            raise ValueError("This task is supposed to fail")

        @task(retries=1, retry_delay=5.0)
        def needs_retry(**context):
            print("This task is supposed to fail on the first attempt")
            if context["ti"].try_number == 1:
                raise ValueError("This task is supposed to fail")

        @task(trigger_rule=TriggerRule.ONE_SUCCESS)
        def capture_fail():
            print("all good, we accept the fail and report OK")

        [plan_to_fail(), needs_retry()] >> capture_fail()

    @task
    def long_running():
        print("This task runs for 15 minutes")
        for i in range(15):
            sleep(60)
            print(f"Running for {i + 1} minutes now.")
        print("Long running task completed.")

    @task
    def my_teardown():
        print("Assume this is a teardown task")

    (
        my_setup().as_setup()
        >> [mapping_task_group(), standard_tasks_group(), failure_tests_group(), long_running()]
        >> my_teardown().as_teardown()
    )
