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
"""Airflow models."""

from __future__ import annotations

# Do not add new models to this -- this is for compat only
__all__ = [
    "DAG",
    "ID_LEN",
    "Base",
    "BaseOperator",
    "BaseOperatorLink",
    "Connection",
    "DagBag",
    "DagWarning",
    "DagModel",
    "DagPickle",
    "DagRun",
    "DagTag",
    "DbCallbackRequest",
    "Log",
    "MappedOperator",
    "Operator",
    "Param",
    "Pool",
    "RenderedTaskInstanceFields",
    "SkipMixin",
    "SlaMiss",
    "TaskFail",
    "TaskInstance",
    "TaskReschedule",
    "Trigger",
    "Variable",
    "XCom",
    "clear_task_instances",
]

from typing import TYPE_CHECKING


def import_all_models():
    for name in __lazy_imports:
        __getattr__(name)

    import airflow.models.asset
    import airflow.models.backfill
    import airflow.models.dagwarning
    import airflow.models.errors
    import airflow.models.serialized_dag
    import airflow.models.taskinstancehistory
    import airflow.models.tasklog
    import airflow.providers.fab.auth_manager.models


def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    path = __lazy_imports.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from airflow.utils.module_loading import import_string

    val = import_string(f"{path}.{name}")

    # Store for next time
    globals()[name] = val
    return val


__lazy_imports = {
    "DAG": "airflow.models.dag",
    "ID_LEN": "airflow.models.base",
    "Base": "airflow.models.base",
    "BaseOperator": "airflow.models.baseoperator",
    "BaseOperatorLink": "airflow.models.baseoperatorlink",
    "Connection": "airflow.models.connection",
    "DagBag": "airflow.models.dagbag",
    "DagModel": "airflow.models.dag",
    "DagPickle": "airflow.models.dagpickle",
    "DagRun": "airflow.models.dagrun",
    "DagTag": "airflow.models.dag",
    "DagWarning": "airflow.models.dagwarning",
    "DbCallbackRequest": "airflow.models.db_callback_request",
    "Log": "airflow.models.log",
    "MappedOperator": "airflow.models.mappedoperator",
    "Operator": "airflow.models.operator",
    "Param": "airflow.models.param",
    "Pool": "airflow.models.pool",
    "RenderedTaskInstanceFields": "airflow.models.renderedtifields",
    "SkipMixin": "airflow.models.skipmixin",
    "SlaMiss": "airflow.models.slamiss",
    "TaskFail": "airflow.models.taskfail",
    "TaskInstance": "airflow.models.taskinstance",
    "TaskReschedule": "airflow.models.taskreschedule",
    "Trigger": "airflow.models.trigger",
    "Variable": "airflow.models.variable",
    "XCom": "airflow.models.xcom",
    "clear_task_instances": "airflow.models.taskinstance",
}

if TYPE_CHECKING:
    # I was unable to get mypy to respect a airflow/models/__init__.pyi, so
    # having to resort back to this hacky method
    from airflow.models.base import ID_LEN, Base
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.baseoperatorlink import BaseOperatorLink
    from airflow.models.connection import Connection
    from airflow.models.dag import DAG, DagModel, DagTag
    from airflow.models.dagbag import DagBag
    from airflow.models.dagpickle import DagPickle
    from airflow.models.dagrun import DagRun
    from airflow.models.dagwarning import DagWarning
    from airflow.models.db_callback_request import DbCallbackRequest
    from airflow.models.log import Log
    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.operator import Operator
    from airflow.models.param import Param
    from airflow.models.pool import Pool
    from airflow.models.renderedtifields import RenderedTaskInstanceFields
    from airflow.models.skipmixin import SkipMixin
    from airflow.models.slamiss import SlaMiss
    from airflow.models.taskfail import TaskFail
    from airflow.models.taskinstance import TaskInstance, clear_task_instances
    from airflow.models.taskinstancehistory import TaskInstanceHistory
    from airflow.models.taskreschedule import TaskReschedule
    from airflow.models.trigger import Trigger
    from airflow.models.variable import Variable
    from airflow.models.xcom import XCom
