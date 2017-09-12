# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from builtins import bytes
import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory


class BashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :type output_encoding: output encoding of bash command
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command
        self.logger.info("Tmp dir root location: \n %s", gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                self.logger.info(
                    "Temporary script location: %s",
                    script_location
                )
                self.logger.info("Running command: %s", bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=os.setsid)

                self.sp = sp

                self.logger.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    self.logger.info(line)
                sp.wait()
                self.logger.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        self.logger.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)

