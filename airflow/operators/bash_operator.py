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


import logging
import mmap
import os
import re
import signal
import io
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from builtins import bytes

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
            log_outout=True,
            output_encoding='utf-8',
            output_regex_filter=None,
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push = xcom_push
        self.log_outout = log_outout
        self.output_encoding = output_encoding
        self.output_regex_filter = output_regex_filter
        self.sp = None

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            self.log.info("Tmp dir root location: {0}".format(tmp_dir))
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as cmd_file, \
                    NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as stdout_file, \
                    NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as stderr_file:

                cmd_file.write(bytes(self.bash_command, 'utf_8'))
                cmd_file.flush()
                fname = cmd_file.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script location :{0}".format(script_location))
                logging.info("Running command: " + self.bash_command)
                self.sp = Popen(
                        ['bash', fname],
                        stdout=stdout_file,
                        stderr=stderr_file,
                        cwd=tmp_dir,
                        env=self.env,
                        preexec_fn=os.setsid)

                self.sp.wait()

                exit_msg = "Command exited with return code {0}".format(self.sp.returncode)
                if self.sp.returncode:
                    stderr_output = None
                    with io.open(stderr_file.name, 'r+', encoding=self.output_encoding) as stderr_file_handle:
                        if os.path.getsize(stderr_file.name) > 0:
                            stderr_output = mmap.mmap(stderr_file_handle.fileno(), 0, access=mmap.ACCESS_READ)
                    raise AirflowException("Bash command failed, {0}, error: {1}"
                                           .format(exit_msg, stderr_output))

                logging.info(exit_msg)
                output = None
                if self.output_regex_filter or self.log_outout or self.xcom_push:
                    with io.open(stdout_file.name, 'r+', encoding=self.output_encoding) as stdout_file_handle:
                        if os.path.getsize(stdout_file_handle.name) > 0:
                            output = mmap.mmap(stdout_file_handle.fileno(), 0, access=mmap.ACCESS_READ)
                            if self.output_regex_filter:
                                pattern = self.output_regex_filter.encode("utf-8")
                                try:
                                    re.compile(pattern)
                                except re.error:
                                    raise AirflowException("command executed successfully, "
                                                           "but Invalid regex supplied {0} "
                                                           .format(self.output_regex_filter))

                                filtered_output = re.search(pattern, output)
                                if filtered_output:
                                    return filtered_output.group().decode(self.output_encoding)
                                else:
                                    logging.warning("failed to match on output based on "
                                                    "supplied regex : {0}"
                                                    .format(self.output_regex_filter))

                            if self.log_outout:
                                logging.info("stdout: {0}".format(output))

                            if self.xcom_push:
                                return output.decode(self.output_encoding)
                        else:
                            logging.warning("stdout file: {0} is empty".format(stdout_file.name))
                else:
                    logging.warning("Not logging stdout for the command: {0}"
                                    .format(self.bash_command))

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
