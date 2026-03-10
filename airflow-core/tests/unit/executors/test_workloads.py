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

from pathlib import PurePosixPath
from uuid import uuid4

from airflow.executors import workloads
from airflow.executors.workloads import TaskInstance, TaskInstanceDTO
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.task import ExecuteTask


def test_task_instance_alias_keeps_backwards_compat():
    assert TaskInstance is TaskInstanceDTO
    assert workloads.TaskInstance is TaskInstanceDTO
    assert workloads.TaskInstanceDTO is TaskInstanceDTO


def test_token_excluded_from_workload_repr():
    """Ensure JWT tokens do not leak into log output via repr()."""
    fake_token = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.secret_payload.signature"
    ti = TaskInstanceDTO(
        id=uuid4(),
        dag_version_id=uuid4(),
        task_id="test_task",
        dag_id="test_dag",
        run_id="test_run",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )
    workload = ExecuteTask(
        ti=ti,
        dag_rel_path=PurePosixPath("test_dag.py"),
        token=fake_token,
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="test.log",
    )

    workload_repr = repr(workload)

    # Token MUST NOT appear in repr (prevents leaking into logs)
    assert fake_token not in workload_repr, f"JWT token leaked into repr! Found token in: {workload_repr}"
    # But token should still be accessible as an attribute
    assert workload.token == fake_token
