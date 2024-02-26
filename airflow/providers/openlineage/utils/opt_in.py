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

from typing import TypeVar

from airflow.models import DAG, Operator, Param
from airflow.models.xcom_arg import XComArg

ENABLE_OL_PARAM_NAME = "_enable_ol"
ENABLE_OL_PARAM = Param(True, const=True)
DISABLE_OL_PARAM = Param(False, const=False)
T = TypeVar("T", bound="DAG | Operator")


def enable_lineage(obj: T) -> T:
    if isinstance(obj, XComArg):
        enable_lineage(obj.operator)
        return obj
    # propagate param to tasks
    if isinstance(obj, DAG):
        for task in obj.task_dict.values():
            enable_lineage(task)
    obj.params[ENABLE_OL_PARAM_NAME] = ENABLE_OL_PARAM
    return obj


def disable_lineage(obj: T) -> T:
    if isinstance(obj, XComArg):
        disable_lineage(obj.operator)
        return obj
    # propagate param to tasks
    if isinstance(obj, DAG):
        for task in obj.task_dict.values():
            disable_lineage(task)
    obj.params[ENABLE_OL_PARAM_NAME] = DISABLE_OL_PARAM
    return obj


def is_task_lineage_enabled(task: Operator) -> bool:
    return task.params.get(ENABLE_OL_PARAM_NAME) is True


def is_dag_lineage_enabled(dag: DAG) -> bool:
    return dag.params.get(ENABLE_OL_PARAM_NAME) is True or any(
        is_task_lineage_enabled(task) for task in dag.tasks
    )
