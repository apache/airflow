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

from airflow.utils.logging import LoggingMixin


class BaseTaskRunner(LoggingMixin):
    """
    Runs Airflow task instances by invoking the `airflow run` command with raw
    mode enabled in a subprocess.
    """

    def __init__(self, local_task_job):
        """
        :param local_task_job: The local task job associated with running the
        associated task instance.
        :type local_task_job: airflow.jobs.LocalTaskJob
        """
        self._task_instance = local_task_job.task_instance
        self._command = self._task_instance.command_as_list(
            raw=True,
            ignore_dependencies=local_task_job.ignore_dependencies,
            ignore_depends_on_past=local_task_job.ignore_depends_on_past,
            force=local_task_job.force,
            pickle_id=local_task_job.pickle_id,
            mark_success=local_task_job.mark_success,
            job_id=local_task_job.id,
            pool=local_task_job.pool,
        )

    def start(self):
        """
        Start running the task instance in a subprocess.
        """
        raise NotImplementedError()

    def return_code(self):
        """
        :return: The return code associated with running the task instance or
        None if the task is not yet done.
        :rtype int:
        """
        raise NotImplementedError()

    def terminate(self):
        """
        Kill the running task instance.
        """
        raise NotImplementedError()

    def on_finish(self):
        """
        A callback that should be called when this is done running.
        """
        raise NotImplementedError()
