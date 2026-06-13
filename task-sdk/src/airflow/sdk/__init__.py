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
    "AllowedKeyMapper",
    "Asset",
    "AssetAccessControl",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
    "AssetOrTimeSchedule",
    "AssetWatcher",
    "AsyncCallback",
    "BaseAsyncOperator",
    "BaseBranchOperator",
    "BaseHook",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "BaseXCom",
    "BranchMixIn",
    "ChainMapper",
    "Connection",
    "Context",
    "CronDataIntervalTimetable",
    "CronTriggerTimetable",
    "CronPartitionTimetable",
    "DAG",
    "DagRunState",
    "DayWindow",
    "DeadlineAlert",
    "DeadlineReference",
    "DeltaDataIntervalTimetable",
    "DeltaTriggerTimetable",
    "EdgeModifier",
    "EventsTimetable",
    "ExceptionRetryPolicy",
    "FanOutMapper",
    "FixedKeyMapper",
    "HourWindow",
    "IdentityMapper",
    "Label",
    "Metadata",
    "MinimumCount",
    "MonthWindow",
    "MultipleCronTriggerTimetable",
    "NEVER_EXPIRE",
    "ObjectStoragePath",
    "Param",
    "ParamsDict",
    "PartitionAtRuntime",
    "PartitionedAssetTimetable",
    "PartitionMapper",
    "PokeReturnValue",
    "ProductMapper",
    "QuarterWindow",
    "ResumableJobMixin",
    "RetryAction",
    "RetryDecision",
    "RetryPolicy",
    "RetryRule",
    "RollupMapper",
    "SegmentWindow",
    "SkipMixin",
    "SyncCallback",
    "StartOfDayMapper",
    "StartOfHourMapper",
    "StartOfMonthMapper",
    "StartOfQuarterMapper",
    "StartOfWeekMapper",
    "StartOfYearMapper",
    "TaskGroup",
    "TaskInstance",
    "TaskInstanceState",
    "TriggerRule",
    "Variable",
    "WaitForAll",
    "WeekWindow",
    "WeightRule",
    "Window",
    "XComArg",
    "YearWindow",
    "asset",
    "chain",
    "chain_linear",
    "conf",
    "cross_downstream",
    "dag",
    "get_current_context",
    "get_parsing_context",
    "literal",
    "lineage",
    "macros",
    "setup",
    "task",
    "task_group",
    "teardown",
]

__version__ = "1.3.0"

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import DagRunState, TaskInstanceState, TriggerRule, WeightRule
    from airflow.sdk.bases.branch import BaseBranchOperator, BranchMixIn
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
    from airflow.sdk.bases.resumablejobmixin import ResumableJobMixin
    from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
    from airflow.sdk.bases.skipmixin import SkipMixin
    from airflow.sdk.bases.xcom import BaseXCom
    from airflow.sdk.configuration import AirflowSDKConfigParser
    from airflow.sdk.definitions.asset import (
        Asset,
        AssetAccessControl,
        AssetAlias,
        AssetAll,
        AssetAny,
        AssetWatcher,
    )
    from airflow.sdk.definitions.asset.decorators import asset
    from airflow.sdk.definitions.asset.metadata import Metadata
    from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context, get_current_context, get_parsing_context
    from airflow.sdk.definitions.dag import DAG, dag
    from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
    from airflow.sdk.definitions.decorators import setup, task, teardown
    from airflow.sdk.definitions.decorators.task_group import task_group
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.param import Param, ParamsDict
    from airflow.sdk.definitions.partition_mappers.allowed_key import AllowedKeyMapper
    from airflow.sdk.definitions.partition_mappers.base import (
        PartitionMapper,
        RollupMapper,
    )
    from airflow.sdk.definitions.partition_mappers.chain import ChainMapper
    from airflow.sdk.definitions.partition_mappers.fixed_key import FixedKeyMapper
    from airflow.sdk.definitions.partition_mappers.identity import IdentityMapper
    from airflow.sdk.definitions.partition_mappers.product import ProductMapper
    from airflow.sdk.definitions.partition_mappers.temporal import (
        FanOutMapper,
        StartOfDayMapper,
        StartOfHourMapper,
        StartOfMonthMapper,
        StartOfQuarterMapper,
        StartOfWeekMapper,
        StartOfYearMapper,
    )
    from airflow.sdk.definitions.partition_mappers.wait_policy import (
        MinimumCount,
        WaitForAll,
    )
    from airflow.sdk.definitions.partition_mappers.window import (
        DayWindow,
        HourWindow,
        MonthWindow,
        QuarterWindow,
        SegmentWindow,
        WeekWindow,
        Window,
        YearWindow,
    )
    from airflow.sdk.definitions.retry_policy import (
        ExceptionRetryPolicy,
        RetryAction,
        RetryDecision,
        RetryPolicy,
        RetryRule,
    )
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.definitions.template import literal
    from airflow.sdk.definitions.timetables.assets import (
        AssetOrTimeSchedule,
        PartitionAtRuntime,
        PartitionedAssetTimetable,
    )
    from airflow.sdk.definitions.timetables.events import EventsTimetable
    from airflow.sdk.definitions.timetables.interval import (
        CronDataIntervalTimetable,
        DeltaDataIntervalTimetable,
    )
    from airflow.sdk.definitions.timetables.trigger import (
        CronPartitionTimetable,
        CronTriggerTimetable,
        DeltaTriggerTimetable,
        MultipleCronTriggerTimetable,
    )
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.definitions.xcom_arg import XComArg
    from airflow.sdk.execution_time import macros
    from airflow.sdk.execution_time.context import NEVER_EXPIRE
    from airflow.sdk.io.path import ObjectStoragePath
    from airflow.sdk.types import TaskInstance

    conf: AirflowSDKConfigParser

__lazy_imports: dict[str, str] = {
    "AllowedKeyMapper": ".definitions.partition_mappers.allowed_key",
    "Asset": ".definitions.asset",
    "AssetAccessControl": ".definitions.asset",
    "AssetAlias": ".definitions.asset",
    "AssetAll": ".definitions.asset",
    "AssetAny": ".definitions.asset",
    "AssetOrTimeSchedule": ".definitions.timetables.assets",
    "AssetWatcher": ".definitions.asset",
    "AsyncCallback": ".definitions.callback",
    "BaseAsyncOperator": ".bases.operator",
    "BaseBranchOperator": ".bases.branch",
    "BaseHook": ".bases.hook",
    "BaseNotifier": ".bases.notifier",
    "BaseOperator": ".bases.operator",
    "BaseOperatorLink": ".bases.operatorlink",
    "BaseSensorOperator": ".bases.sensor",
    "BaseXCom": ".bases.xcom",
    "BranchMixIn": ".bases.branch",
    "ChainMapper": ".definitions.partition_mappers.chain",
    "Connection": ".definitions.connection",
    "Context": ".definitions.context",
    "CronDataIntervalTimetable": ".definitions.timetables.interval",
    "CronTriggerTimetable": ".definitions.timetables.trigger",
    "CronPartitionTimetable": ".definitions.timetables.trigger",
    "DAG": ".definitions.dag",
    "DagRunState": ".api.datamodels._generated",
    "DayWindow": ".definitions.partition_mappers.window",
    "DeadlineAlert": ".definitions.deadline",
    "DeadlineReference": ".definitions.deadline",
    "DeltaDataIntervalTimetable": ".definitions.timetables.interval",
    "DeltaTriggerTimetable": ".definitions.timetables.trigger",
    "EdgeModifier": ".definitions.edges",
    "EventsTimetable": ".definitions.timetables.events",
    "ExceptionRetryPolicy": ".definitions.retry_policy",
    "FanOutMapper": ".definitions.partition_mappers.temporal",
    "FixedKeyMapper": ".definitions.partition_mappers.fixed_key",
    "HourWindow": ".definitions.partition_mappers.window",
    "IdentityMapper": ".definitions.partition_mappers.identity",
    "Label": ".definitions.edges",
    "Metadata": ".definitions.asset.metadata",
    "MinimumCount": ".definitions.partition_mappers.wait_policy",
    "MonthWindow": ".definitions.partition_mappers.window",
    "MultipleCronTriggerTimetable": ".definitions.timetables.trigger",
    "ObjectStoragePath": ".io.path",
    "Param": ".definitions.param",
    "ParamsDict": ".definitions.param",
    "PartitionAtRuntime": ".definitions.timetables.assets",
    "PartitionedAssetTimetable": ".definitions.timetables.assets",
    "PartitionMapper": ".definitions.partition_mappers.base",
    "PokeReturnValue": ".bases.sensor",
    "ProductMapper": ".definitions.partition_mappers.product",
    "QuarterWindow": ".definitions.partition_mappers.window",
    "ResumableJobMixin": ".bases.resumablejobmixin",
    "RetryAction": ".definitions.retry_policy",
    "RetryDecision": ".definitions.retry_policy",
    "RetryPolicy": ".definitions.retry_policy",
    "RetryRule": ".definitions.retry_policy",
    "RollupMapper": ".definitions.partition_mappers.base",
    "SecretCache": ".execution_time.cache",
    "SegmentWindow": ".definitions.partition_mappers.window",
    "SkipMixin": ".bases.skipmixin",
    "SyncCallback": ".definitions.callback",
    "StartOfDayMapper": ".definitions.partition_mappers.temporal",
    "StartOfHourMapper": ".definitions.partition_mappers.temporal",
    "StartOfMonthMapper": ".definitions.partition_mappers.temporal",
    "StartOfQuarterMapper": ".definitions.partition_mappers.temporal",
    "StartOfWeekMapper": ".definitions.partition_mappers.temporal",
    "StartOfYearMapper": ".definitions.partition_mappers.temporal",
    "TaskGroup": ".definitions.taskgroup",
    "TaskInstance": ".types",
    "TaskInstanceState": ".api.datamodels._generated",
    "TriggerRule": ".api.datamodels._generated",
    "Variable": ".definitions.variable",
    "WaitForAll": ".definitions.partition_mappers.wait_policy",
    "WeekWindow": ".definitions.partition_mappers.window",
    "WeightRule": ".api.datamodels._generated",
    "Window": ".definitions.partition_mappers.window",
    "XComArg": ".definitions.xcom_arg",
    "YearWindow": ".definitions.partition_mappers.window",
    "asset": ".definitions.asset.decorators",
    "chain": ".bases.operator",
    "chain_linear": ".bases.operator",
    "conf": ".configuration",
    "cross_downstream": ".bases.operator",
    "dag": ".definitions.dag",
    "NEVER_EXPIRE": ".execution_time.context",
    "get_current_context": ".definitions.context",
    "get_parsing_context": ".definitions.context",
    "literal": ".definitions.template",
    "lineage": ".lineage",
    "macros": ".execution_time",
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
