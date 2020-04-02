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

# pylint: disable=missing-docstring

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner

_TASK_RUNNER = conf.get('core', 'TASK_RUNNER')


def get_task_runner(local_task_job):
    """
    Get the task runner that can be used to run the given job.

    :param local_task_job: The LocalTaskJob associated with the TaskInstance
        that needs to be executed.
    :type local_task_job: airflow.jobs.local_task_job.LocalTaskJob
    :return: The task runner to use to run the task.
    :rtype: airflow.task.task_runner.base_task_runner.BaseTaskRunner
    """
    if _TASK_RUNNER == "StandardTaskRunner":
        return StandardTaskRunner(local_task_job)
    elif _TASK_RUNNER == "CgroupTaskRunner":
        from airflow.task.task_runner.cgroup_task_runner import CgroupTaskRunner
        return CgroupTaskRunner(local_task_job)
    else:
        raise AirflowException("Unknown task runner type {}".format(_TASK_RUNNER))
