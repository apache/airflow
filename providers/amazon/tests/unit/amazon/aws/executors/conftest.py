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

from uuid import uuid4

import pytest


@pytest.fixture
def task_identity_workloads():
    from airflow.executors.workloads import BundleInfo, ExecuteTask, TaskInstanceDTO

    first = ExecuteTask(
        ti=TaskInstanceDTO(
            id=uuid4(),
            dag_version_id=uuid4(),
            dag_id="dag",
            task_id="task",
            run_id="run",
            try_number=1,
            pool_slots=1,
            priority_weight=1,
            queue="default",
        ),
        dag_rel_path="dag.py",
        bundle_info=BundleInfo(name="bundle"),
        token="",
        log_path=None,
    )
    return [first, first.model_copy(update={"ti": first.ti.model_copy(update={"id": uuid4()})})]
