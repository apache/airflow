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
"""Airflow models"""
from typing import Union

# Do not add new models to this -- this is for compat only
__all__ = [
    "DAG",
    "ID_LEN",
    "XCOM_RETURN_KEY",
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
    "ImportError",
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


def import_all_models():
    for name in __lazy_imports:
        __getattr__(name)

    import airflow.jobs.backfill_job
    import airflow.jobs.base_job
    import airflow.jobs.local_task_job
    import airflow.jobs.scheduler_job
    import airflow.jobs.triggerer_job
    import airflow.models.serialized_dag
    import airflow.models.tasklog
    import airflow.models.dagwarning
    import airflow.models.dataset


def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    path = __lazy_imports.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from airflow.utils.module_loading import import_string

    val = import_string(f'{path}.{name}')
    # Store for next time
    globals()[name] = val
    return val


__lazy_imports = {
    'DAG': 'airflow.models.dag',
    'ID_LEN': 'airflow.models.base',
    'XCOM_RETURN_KEY': 'airflow.models.xcom',
    'Base': 'airflow.models.base',
    'BaseOperator': 'airflow.models.baseoperator',
    'BaseOperatorLink': 'airflow.models.baseoperator',
    'Connection': 'airflow.models.connection',
    'DagBag': 'airflow.models.dagbag',
    'DagModel': 'airflow.models.dag',
    'DagPickle': 'airflow.models.dagpickle',
    'DagRun': 'airflow.models.dagrun',
    'DagTag': 'airflow.models.dag',
    'DbCallbackRequest': 'airflow.models.db_callback_request',
    'ImportError': 'airflow.models.errors',
    'Log': 'airflow.models.log',
    'MappedOperator': 'airflow.models.mappedoperator',
    'Operator': 'airflow.models.operator',
    'Param': 'airflow.models.param',
    'Pool': 'airflow.models.pool',
    'RenderedTaskInstanceFields': 'airflow.models.renderedtifields',
    'SkipMixin': 'airflow.models.skipmixin',
    'SlaMiss': 'airflow.models.slamiss',
    'TaskFail': 'airflow.models.taskfail',
    'TaskInstance': 'airflow.models.taskinstance',
    'TaskReschedule': 'airflow.models.taskreschedule',
    'Trigger': 'airflow.models.trigger',
    'Variable': 'airflow.models.variable',
    'XCom': 'airflow.models.xcom',
    'clear_task_instances': 'airflow.models.taskinstance',
}
