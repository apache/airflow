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


class ROperator(BaseOperator):
    """
    Execute an R script, command, or set of commands

    :param r_command: The command, set of commands, or a reference to an R
        script (must have '.r' extension) to be executed (templated)
    :type r_command: string
    :param rscript_bin: The command to run to execute an R script (default:
        'Rscript'). If Rscript is not in the PATH, the full path can be
        specified. Alternate interpreters can also be used (e.g., littler) by
        specifying their executable here.
    :type rscript_bin: string
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. Note that this will remove PATH, which makes it impossible
        to find `rscript_bin`. Either give the full path for `rscript_bin`,
        or add `'PATH': os.environ['PATH']` to the given env. (templated)
    :type env: dict
    :param xcom_push: If xcom_push is True (default: False), the last line
        written to stdout will also be pushed to an XCom (key 'return_value')
        when the R command completes.
    :type xcom_push: bool
    :param output_encoding: encoding output from R (default: 'utf-8')
    :type output_encoding: string

    """

    template_fields = ('r_command', 'env')
    template_ext = ('.r', '.R')
    ui_color = '#C8D5E6'

    @apply_defaults
    def __init__(
            self,
            r_command,
            rscript_bin='Rscript',
            env=None,
            xcom_push=False,
            output_encoding='utf-8',
            *args, **kwargs):

        super(ROperator, self).__init__(*args, **kwargs)
        self.r_command = r_command
        self.rscript_bin = rscript_bin
        self.env = env
        self.xcom_push = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the R command in a temporary directory
        """

        self.log.info("Tmp dir root location: \n %s", gettempdir())

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.r_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info("Temporary script location: %s", script_location)

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command(s): %s", self.r_command)

                sp = Popen(
                    args=[self.rscript_bin, fname],
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=tmp_dir,
                    env=self.env,
                    preexec_fn=pre_exec,
                )

                self.sp = sp

                self.log.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)

                sp.wait()
                self.log.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException('R command failed')

        if self.xcom_push:
            self.log.info('Pushing last line of output to Xcom')
            return line

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to R process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
