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
"""Standard task runner"""
import psutil

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group


class StandardTaskRunner(BaseTaskRunner):
    """
    Standard runner for all tasks.
    """
    def __init__(self, local_task_job):
        super().__init__(local_task_job)

    def start(self):
        self.process = self.run_command()

    def return_code(self):
        return self.process.poll()

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log)
