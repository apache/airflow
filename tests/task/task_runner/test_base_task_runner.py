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

from unittest import mock

import pytest

from airflow.jobs.job import Job
from airflow.jobs.local_task_job_runner import LocalTaskJobRunner
from airflow.models.baseoperator import BaseOperator
from airflow.task.task_runner.base_task_runner import BaseTaskRunner


@pytest.mark.parametrize(["impersonation"], (("nobody",), (None,)))
@mock.patch("subprocess.check_call")
@mock.patch("airflow.task.task_runner.base_task_runner.tmp_configuration_copy")
def test_config_copy_mode(tmp_configuration_copy, subprocess_call, dag_maker, impersonation):
    tmp_configuration_copy.return_value = "/tmp/some-string"

    with dag_maker("test"):
        BaseOperator(task_id="task_1", run_as_user=impersonation)

    dr = dag_maker.create_dagrun()

    ti = dr.task_instances[0]
    job = Job(dag_id=ti.dag_id)
    job_runner = LocalTaskJobRunner(job=job, task_instance=ti)
    runner = BaseTaskRunner(job_runner)
    # So we don't try to delete it -- cos the file won't exist
    del runner._cfg_path

    includes = bool(impersonation)

    tmp_configuration_copy.assert_called_with(chmod=0o600, include_env=includes, include_cmds=includes)

    if impersonation:
        subprocess_call.assert_called_with(
            ["sudo", "chown", impersonation, "/tmp/some-string"], close_fds=True
        )
    else:
        subprocess_call.not_assert_called()
