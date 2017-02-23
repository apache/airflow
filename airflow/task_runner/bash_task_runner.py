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

import psutil

from airflow.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import kill_process_tree


class BashTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super(BashTaskRunner, self).__init__(local_task_job)

    def start(self):
        self.process = self.run_command(['bash', '-c'], join_args=True)

    def return_code(self):
        return self.process.poll()

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            kill_process_tree(self.logger, self.process.pid)

    def on_finish(self):
        super(BashTaskRunner, self).on_finish()
