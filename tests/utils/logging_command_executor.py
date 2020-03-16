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
import subprocess

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class LoggingCommandExecutor(LoggingMixin):

    def execute_cmd(self, cmd, silent=False, cwd=None, env=None):
        if silent:
            self.log.info("Executing in silent mode: '%s'", " ".join(cmd))
            with open(os.devnull, 'w') as dev_null:
                return subprocess.call(args=cmd, stdout=dev_null, stderr=subprocess.STDOUT, env=env)
        else:
            self.log.info("Executing: '%s'", " ".join(cmd))
            process = subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, cwd=cwd, env=env
            )
            output, err = process.communicate()
            retcode = process.poll()
            self.log.info("Stdout: %s", output)
            self.log.info("Stderr: %s", err)
            if retcode:
                self.log.warning("Error when executing %s", " ".join(cmd))
            return retcode

    def check_output(self, cmd):
        self.log.info("Executing for output: '%s'", " ".join(cmd))
        process = subprocess.Popen(args=cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output, err = process.communicate()
        retcode = process.poll()
        if retcode:
            self.log.info("Error when executing '%s'", " ".join(cmd))
            self.log.info("Stdout: %s", output)
            self.log.info("Stderr: %s", err)
            raise AirflowException("Retcode {} on {} with stdout: {}, stderr: {}".
                                   format(retcode, " ".join(cmd), output, err))
        return output


def get_executor() -> LoggingCommandExecutor:
    return LoggingCommandExecutor()
