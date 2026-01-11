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

from airflow.providers.common.compat.openlineage.facet import RunFacet

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import TaskInstance, TaskInstanceState


@attrs.define
class MyCustomRunFacet(RunFacet):
    """Define a custom run facet."""

    name: str
    cluster: str


def get_additional_test_facet(
    task_instance: TaskInstance, ti_state: TaskInstanceState
) -> dict[str, RunFacet] | None:
    operator_name = task_instance.task.operator_name if task_instance.task else ""
    if operator_name == "BashOperator":
        return None
    return {
        "additional_run_facet": MyCustomRunFacet(
            name=f"test-lineage-namespace-{ti_state}",
            cluster=f"TEST_{task_instance.dag_id}.{task_instance.task_id}",
        )
    }


def get_duplicate_test_facet_key(
    task_instance: TaskInstance, ti_state: TaskInstanceState
) -> dict[str, RunFacet] | None:
    return get_additional_test_facet(task_instance, ti_state)


def get_another_test_facet(task_instance, ti_state):
    return {"another_run_facet": {"name": "another-lineage-namespace"}}


def return_type_is_not_dict(task_instance, ti_state):
    return "return type is not dict"


def get_custom_facet_throws_exception(task_instance, ti_state):
    raise Exception("fake exception from custom facet function")
