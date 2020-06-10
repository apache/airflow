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


import os
import signal
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
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
    :type bash_command: str
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str

    On execution of this operator the task will be up for retry
    when exception is raised. However, if a sub-command exits with non-zero
    value Airflow will not recognize it as failure unless the whole shell exits
    with a failure. The easiest way of achieving this is to prefix the command
    with ``set -e;``
    Example:

    .. code-block:: python

        bash_command = "set -e; python3 script.py '{{ next_execution_date }}'"

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
            bash_command='echo "here is the message: \'$message\'"',
            env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
        )

    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command: str,
            env: Optional[Dict[str, str]] = None,
            output_encoding: str = 'utf-8',
            *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")
        self.sub_process = None

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info('Tmp dir root location: \n %s', gettempdir())

        # Prepare env for child process.
        env = self.env
        if env is None:
            env = os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug('Exporting the following env vars:\n%s',
                       '\n'.join(["{}={}".format(k, v)
                                  for k, v in airflow_context_vars.items()]))
        env.update(airflow_context_vars)

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info('Running command: %s', self.bash_command)

            self.sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                ['bash', "-c", self.bash_command],
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir,
                env=env,
                preexec_fn=pre_exec)

            self.log.info('Output:')
            line = ''
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode(self.output_encoding).rstrip()
                self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info('Command exited with return code %s', self.sub_process.returncode)

            if self.sub_process.returncode != 0:
                raise AirflowException('Bash command failed. The command returned a non-zero exit code.')

        return line

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
