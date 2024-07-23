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

from typing import TYPE_CHECKING

import attrs

from airflow.providers.common.compat.openlineage.facet import BaseFacet

if TYPE_CHECKING:
    from airflow.models import TaskInstance


@attrs.define(slots=False)
class MyCustomRunFacet(BaseFacet):
    """Define a custom run facet."""

    name: str
    jobState: str
    uniqueName: str
    displayName: str
    dagId: str
    taskId: str
    cluster: str


def get_additional_test_facet(task_instance: TaskInstance) -> dict[str, dict] | None:
    operator_name = task_instance.task.operator_name if task_instance.task else None
    if operator_name == "BashOperator":
        return None
    job_unique_name = f"TEST.{task_instance.dag_id}.{task_instance.task_id}"
    return {
        "additional_run_facet": attrs.asdict(
            MyCustomRunFacet(
                name="test-lineage-namespace",
                jobState=task_instance.state,
                uniqueName=job_unique_name,
                displayName=f"{task_instance.dag_id}.{task_instance.task_id}",
                dagId=task_instance.dag_id,
                taskId=task_instance.task_id,
                cluster="TEST",
            )
        )
    }


def get_duplicate_test_facet_key(task_instance: TaskInstance):
    job_unique_name = f"TEST.{task_instance.dag_id}.{task_instance.task_id}"
    return {
        "additional_run_facet": attrs.asdict(
            MyCustomRunFacet(
                name="test-lineage-namespace",
                jobState=task_instance.state,
                uniqueName=job_unique_name,
                displayName=f"{task_instance.dag_id}.{task_instance.task_id}",
                dagId=task_instance.dag_id,
                taskId=task_instance.task_id,
                cluster="TEST",
            )
        )
    }


def get_another_test_facet(task_instance: TaskInstance):
    return {"another_run_facet": {"name": "another-lineage-namespace"}}


def return_type_is_not_dict(task_instance: TaskInstance):
    return "return type is not dict"


def get_custom_facet_throws_exception(task_instance: TaskInstance):
    raise Exception("fake exception from custom fcet function")
