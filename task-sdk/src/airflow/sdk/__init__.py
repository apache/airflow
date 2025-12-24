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

__all__ = [
    "__version__",
    "Asset",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
    "AssetOrTimeSchedule",
    "AssetWatcher",
    "BaseAsyncOperator",
    "BaseHook",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "Connection",
    "Context",
    "CronDataIntervalTimetable",
    "CronTriggerTimetable",
    "DAG",
    "DagRunState",
    "DeltaDataIntervalTimetable",
    "DeltaTriggerTimetable",
    "EdgeModifier",
    "EventsTimetable",
    "Label",
    "Metadata",
    "MultipleCronTriggerTimetable",
    "ObjectStoragePath",
    "Param",
    "ParamsDict",
    "PokeReturnValue",
    "TaskGroup",
    "TaskInstanceState",
    "Trace",
    "TriggerRule",
    "Variable",
    "WeightRule",
    "XComArg",
    "asset",
    "chain",
    "chain_linear",
    "conf",
    "cross_downstream",
    "dag",
    "get_current_context",
    "get_parsing_context",
    "literal",
    "setup",
    "task",
    "task_group",
    "teardown",
]

__version__ = "1.2.0"

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import DagRunState, TaskInstanceState, TriggerRule, WeightRule
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk.bases.notifier import BaseNotifier
    from airflow.sdk.bases.operator import (
        BaseAsyncOperator,
        BaseOperator,
        chain,
        chain_linear,
        cross_downstream,
    )
    from airflow.sdk.bases.operatorlink import BaseOperatorLink
    from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
    from airflow.sdk.configuration import AirflowSDKConfigParser
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher
    from airflow.sdk.definitions.asset.decorators import asset
    from airflow.sdk.definitions.asset.metadata import Metadata
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context, get_current_context, get_parsing_context
    from airflow.sdk.definitions.dag import DAG, dag
    from airflow.sdk.definitions.decorators import setup, task, teardown
    from airflow.sdk.definitions.decorators.task_group import task_group
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.param import Param, ParamsDict
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.definitions.template import literal
    from airflow.sdk.definitions.timetables.assets import AssetOrTimeSchedule
    from airflow.sdk.definitions.timetables.events import EventsTimetable
    from airflow.sdk.definitions.timetables.interval import (
        CronDataIntervalTimetable,
        DeltaDataIntervalTimetable,
    )
    from airflow.sdk.definitions.timetables.trigger import (
        CronTriggerTimetable,
        DeltaTriggerTimetable,
        MultipleCronTriggerTimetable,
    )
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.definitions.xcom_arg import XComArg
    from airflow.sdk.io.path import ObjectStoragePath
    from airflow.sdk.observability.trace import Trace

    conf: AirflowSDKConfigParser

__lazy_imports: dict[str, str] = {
    "Asset": ".definitions.asset",
    "AssetAlias": ".definitions.asset",
    "AssetAll": ".definitions.asset",
    "AssetAny": ".definitions.asset",
    "AssetOrTimeSchedule": ".definitions.timetables.assets",
    "AssetWatcher": ".definitions.asset",
    "BaseAsyncOperator": ".bases.operator",
    "BaseHook": ".bases.hook",
    "BaseNotifier": ".bases.notifier",
    "BaseOperator": ".bases.operator",
    "BaseOperatorLink": ".bases.operatorlink",
    "BaseSensorOperator": ".bases.sensor",
    "Connection": ".definitions.connection",
    "Context": ".definitions.context",
    "CronDataIntervalTimetable": ".definitions.timetables.interval",
    "CronTriggerTimetable": ".definitions.timetables.trigger",
    "DAG": ".definitions.dag",
    "DagRunState": ".api.datamodels._generated",
    "DeltaDataIntervalTimetable": ".definitions.timetables.interval",
    "DeltaTriggerTimetable": ".definitions.timetables.trigger",
    "EdgeModifier": ".definitions.edges",
    "EventsTimetable": ".definitions.timetables.events",
    "Label": ".definitions.edges",
    "Metadata": ".definitions.asset.metadata",
    "MultipleCronTriggerTimetable": ".definitions.timetables.trigger",
    "ObjectStoragePath": ".io.path",
    "Param": ".definitions.param",
    "ParamsDict": ".definitions.param",
    "PokeReturnValue": ".bases.sensor",
    "SecretCache": ".execution_time.cache",
    "TaskGroup": ".definitions.taskgroup",
    "TaskInstanceState": ".api.datamodels._generated",
    "Trace": ".observability.trace",
    "TriggerRule": ".api.datamodels._generated",
    "Variable": ".definitions.variable",
    "WeightRule": ".api.datamodels._generated",
    "XComArg": ".definitions.xcom_arg",
    "asset": ".definitions.asset.decorators",
    "chain": ".bases.operator",
    "chain_linear": ".bases.operator",
    "conf": ".configuration",
    "cross_downstream": ".bases.operator",
    "dag": ".definitions.dag",
    "get_current_context": ".definitions.context",
    "get_parsing_context": ".definitions.context",
    "literal": ".definitions.template",
    "setup": ".definitions.decorators",
    "task": ".definitions.decorators",
    "task_group": ".definitions.decorators",
    "teardown": ".definitions.decorators",
}


def __getattr__(name: str):
    if module_path := __lazy_imports.get(name):
        import importlib

        mod = importlib.import_module(module_path, __name__)
        val = getattr(mod, name)

        # Store for next time
        globals()[name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
