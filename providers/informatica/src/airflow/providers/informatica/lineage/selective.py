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

from airflow.providers.common.compat.sdk import DAG, Param, XComArg

_DISABLE_PARAM_NAME = "_informatica_auto_lineage"
_ENABLE_PARAM = Param(True, const=True)
_DISABLE_PARAM = Param(False, const=False)


def disable_informatica_lineage(obj):
    """
    Mark a task (or all tasks in a DAG) to skip automatic lineage detection.

    Has no effect on manually declared inlets and outlets.
    """
    if isinstance(obj, XComArg):
        disable_informatica_lineage(obj.operator)
        return obj
    if isinstance(obj, DAG):
        for task in obj.task_dict.values():
            disable_informatica_lineage(task)
    else:
        obj.params[_DISABLE_PARAM_NAME] = _DISABLE_PARAM
    return obj


def enable_informatica_lineage(obj):
    """
    Re-enable automatic lineage detection on a task (or all tasks in a DAG).

    Only needed to reverse a prior ``disable_informatica_lineage`` call.
    """
    if isinstance(obj, XComArg):
        enable_informatica_lineage(obj.operator)
        return obj
    if isinstance(obj, DAG):
        for task in obj.task_dict.values():
            enable_informatica_lineage(task)
    else:
        obj.params[_DISABLE_PARAM_NAME] = _ENABLE_PARAM
    return obj


def is_task_auto_lineage_disabled(task) -> bool:
    """Return True when auto lineage has been explicitly disabled on this task."""
    params = getattr(task, "params", None)
    if params is None:
        return False
    value = params.get(_DISABLE_PARAM_NAME)
    if isinstance(value, Param):
        return value.resolve(suppress_exception=True) is False
    return value is False
