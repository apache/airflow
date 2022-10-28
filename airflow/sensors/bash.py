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
from subprocess import PIPE, STDOUT, Popen
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir
from typing import Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class BashSensor(BaseSensorOperator):
    """
    Executes a bash command/script and returns True if and only if the
    return code is 0.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.

    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param output_encoding: output encoding of bash command.

    .. seealso::
        For more information on how to use this sensor,take a look at the guide:
        :ref:`howto/operator:BashSensor`

    """

    template_fields: Sequence[str] = ('bash_command', 'env')

    def __init__(self, *, bash_command, env=None, output_encoding='utf-8', **kwargs):
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding

    def poke(self, context: Context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command
        self.log.info("Tmp dir root location: \n %s", gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:
                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                self.log.info("Temporary script location: %s", script_location)
                self.log.info("Running command: %s", bash_command)

                with Popen(
                    ['bash', fname],
                    stdout=PIPE,
                    stderr=STDOUT,
                    close_fds=True,
                    cwd=tmp_dir,
                    env=self.env,
                    preexec_fn=os.setsid,
                ) as resp:
                    if resp.stdout:
                        self.log.info("Output:")
                        for line in iter(resp.stdout.readline, b''):
                            self.log.info(line.decode(self.output_encoding).strip())
                    resp.wait()
                    self.log.info("Command exited with return code %s", resp.returncode)

                    return not resp.returncode
