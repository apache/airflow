# -*- coding: utf-8 -*-
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

from airflow import LoggingMixin, AirflowException


class LoggingCommandExecutor(LoggingMixin):

    def execute_cmd(self, cmd, silent=False, cwd=None):
        if silent:
            self.log.info("Executing in silent mode: '{}'".format(" ".join(cmd)))
            with open(os.devnull, 'w') as FNULL:
                return subprocess.call(args=cmd, stdout=FNULL, stderr=subprocess.STDOUT)
        else:
            self.log.info("Executing: '{}'".format(" ".join(cmd)))
            process = subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, cwd=cwd
            )
            output, err = process.communicate()
            retcode = process.poll()
            self.log.info("Stdout: {}".format(output))
            self.log.info("Stderr: {}".format(err))
            if retcode:
                self.log.warning("Error when executing %s", " ".join(cmd))
            return retcode

    def check_output(self, cmd):
        self.log.info("Executing for output: '{}'".format(" ".join(cmd)))
        process = subprocess.Popen(args=cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output, err = process.communicate()
        retcode = process.poll()
        if retcode:
            self.log.info("Error when executing '{}'".format(" ".join(cmd)))
            self.log.info("Stdout: {}".format(output))
            self.log.info("Stderr: {}".format(err))
            raise AirflowException("Retcode {} on {} with stdout: {}, stderr: {}".
                                   format(retcode, " ".join(cmd), output, err))
        return output
