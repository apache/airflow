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
    "AssetAndTimeSchedule",
    "AssetOrTimeSchedule",
    "AssetTriggeredTimetable",
    "AssetWatcher",
    "AsyncCallback",
    "BaseAsyncOperator",
    "BaseBranchOperator",
    "BaseDeadlineReference",
    "BaseHook",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "BaseXCom",
    "BranchMixIn",
    "ChainMapper",
    "ChainRetryPolicy",
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
    "LoopDecision",
    "Metadata",
    "MinimumCount",
    "MonthWindow",
    "MultipleCronTriggerTimetable",
    "NEVER_EXPIRE",
    "ObjectStoragePath",
    "Param",
    "ParamsDict",
    "PartitionedAtRuntime",
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
    "deadline_reference",
    "get_current_context",
    "get_parsing_context",
    "literal",
    "lineage",
    "macros",
    "result",
    "setup",
    "task",
    "task_group",
    "teardown",
]

__version__ = "1.4.0"

if TYPE_CHECKING:
    from airflow.sdk import lineage
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
    from airflow.sdk.definitions.deadline import (
        BaseDeadlineReference,
        DeadlineAlert,
        DeadlineReference,
        deadline_reference,
    )
    from airflow.sdk.definitions.decorators import result, setup, task, teardown
    from airflow.sdk.definitions.decorators.task_group import task_group
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.loop import LoopDecision
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
        ChainRetryPolicy,
        ExceptionRetryPolicy,
        RetryAction,
        RetryDecision,
        RetryPolicy,
        RetryRule,
    )
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.definitions.template import literal
    from airflow.sdk.definitions.timetables.assets import (
        AssetAndTimeSchedule,
        AssetOrTimeSchedule,
        AssetTriggeredTimetable,
        PartitionedAssetTimetable,
        PartitionedAtRuntime,
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

# Things to lazy import in form {local_name: (target_module, target_name)}.
# An empty target_name exposes the target module itself.
__lazy_imports: dict[str, tuple[str, str]] = {
    "AllowedKeyMapper": (".definitions.partition_mappers.allowed_key", "AllowedKeyMapper"),
    "Asset": (".definitions.asset", "Asset"),
    "AssetAccessControl": (".definitions.asset", "AssetAccessControl"),
    "AssetAlias": (".definitions.asset", "AssetAlias"),
    "AssetAll": (".definitions.asset", "AssetAll"),
    "AssetAndTimeSchedule": (".definitions.timetables.assets", "AssetAndTimeSchedule"),
    "AssetAny": (".definitions.asset", "AssetAny"),
    "AssetOrTimeSchedule": (".definitions.timetables.assets", "AssetOrTimeSchedule"),
    "AssetTriggeredTimetable": (".definitions.timetables.assets", "AssetTriggeredTimetable"),
    "AssetWatcher": (".definitions.asset", "AssetWatcher"),
    "AsyncCallback": (".definitions.callback", "AsyncCallback"),
    "BaseAsyncOperator": (".bases.operator", "BaseAsyncOperator"),
    "BaseBranchOperator": (".bases.branch", "BaseBranchOperator"),
    "BaseDeadlineReference": (".definitions.deadline", "BaseDeadlineReference"),
    "BaseHook": (".bases.hook", "BaseHook"),
    "BaseNotifier": (".bases.notifier", "BaseNotifier"),
    "BaseOperator": (".bases.operator", "BaseOperator"),
    "BaseOperatorLink": (".bases.operatorlink", "BaseOperatorLink"),
    "BaseSensorOperator": (".bases.sensor", "BaseSensorOperator"),
    "BaseXCom": (".bases.xcom", "BaseXCom"),
    "BranchMixIn": (".bases.branch", "BranchMixIn"),
    "ChainMapper": (".definitions.partition_mappers.chain", "ChainMapper"),
    "ChainRetryPolicy": (".definitions.retry_policy", "ChainRetryPolicy"),
    "Connection": (".definitions.connection", "Connection"),
    "Context": (".definitions.context", "Context"),
    "CronDataIntervalTimetable": (".definitions.timetables.interval", "CronDataIntervalTimetable"),
    "CronTriggerTimetable": (".definitions.timetables.trigger", "CronTriggerTimetable"),
    "CronPartitionTimetable": (".definitions.timetables.trigger", "CronPartitionTimetable"),
    "DAG": (".definitions.dag", "DAG"),
    "DagRunState": (".api.datamodels._generated", "DagRunState"),
    "DayWindow": (".definitions.partition_mappers.window", "DayWindow"),
    "DeadlineAlert": (".definitions.deadline", "DeadlineAlert"),
    "DeadlineReference": (".definitions.deadline", "DeadlineReference"),
    "DeltaDataIntervalTimetable": (".definitions.timetables.interval", "DeltaDataIntervalTimetable"),
    "DeltaTriggerTimetable": (".definitions.timetables.trigger", "DeltaTriggerTimetable"),
    "EdgeModifier": (".definitions.edges", "EdgeModifier"),
    "EventsTimetable": (".definitions.timetables.events", "EventsTimetable"),
    "ExceptionRetryPolicy": (".definitions.retry_policy", "ExceptionRetryPolicy"),
    "FanOutMapper": (".definitions.partition_mappers.temporal", "FanOutMapper"),
    "FixedKeyMapper": (".definitions.partition_mappers.fixed_key", "FixedKeyMapper"),
    "HourWindow": (".definitions.partition_mappers.window", "HourWindow"),
    "IdentityMapper": (".definitions.partition_mappers.identity", "IdentityMapper"),
    "Label": (".definitions.edges", "Label"),
    "LoopDecision": (".definitions.loop", "LoopDecision"),
    "Metadata": (".definitions.asset.metadata", "Metadata"),
    "MinimumCount": (".definitions.partition_mappers.wait_policy", "MinimumCount"),
    "MonthWindow": (".definitions.partition_mappers.window", "MonthWindow"),
    "MultipleCronTriggerTimetable": (".definitions.timetables.trigger", "MultipleCronTriggerTimetable"),
    "ObjectStoragePath": (".io.path", "ObjectStoragePath"),
    "Param": (".definitions.param", "Param"),
    "ParamsDict": (".definitions.param", "ParamsDict"),
    "PartitionedAtRuntime": (".definitions.timetables.assets", "PartitionedAtRuntime"),
    "PartitionedAssetTimetable": (".definitions.timetables.assets", "PartitionedAssetTimetable"),
    "PartitionMapper": (".definitions.partition_mappers.base", "PartitionMapper"),
    "PokeReturnValue": (".bases.sensor", "PokeReturnValue"),
    "ProductMapper": (".definitions.partition_mappers.product", "ProductMapper"),
    "QuarterWindow": (".definitions.partition_mappers.window", "QuarterWindow"),
    "ResumableJobMixin": (".bases.resumablejobmixin", "ResumableJobMixin"),
    "RetryAction": (".definitions.retry_policy", "RetryAction"),
    "RetryDecision": (".definitions.retry_policy", "RetryDecision"),
    "RetryPolicy": (".definitions.retry_policy", "RetryPolicy"),
    "RetryRule": (".definitions.retry_policy", "RetryRule"),
    "RollupMapper": (".definitions.partition_mappers.base", "RollupMapper"),
    "SecretCache": (".execution_time.cache", "SecretCache"),
    "SegmentWindow": (".definitions.partition_mappers.window", "SegmentWindow"),
    "SkipMixin": (".bases.skipmixin", "SkipMixin"),
    "SyncCallback": (".definitions.callback", "SyncCallback"),
    "StartOfDayMapper": (".definitions.partition_mappers.temporal", "StartOfDayMapper"),
    "StartOfHourMapper": (".definitions.partition_mappers.temporal", "StartOfHourMapper"),
    "StartOfMonthMapper": (".definitions.partition_mappers.temporal", "StartOfMonthMapper"),
    "StartOfQuarterMapper": (".definitions.partition_mappers.temporal", "StartOfQuarterMapper"),
    "StartOfWeekMapper": (".definitions.partition_mappers.temporal", "StartOfWeekMapper"),
    "StartOfYearMapper": (".definitions.partition_mappers.temporal", "StartOfYearMapper"),
    "TaskGroup": (".definitions.taskgroup", "TaskGroup"),
    "TaskInstance": (".types", "TaskInstance"),
    "TaskInstanceState": (".api.datamodels._generated", "TaskInstanceState"),
    "TriggerRule": (".api.datamodels._generated", "TriggerRule"),
    "Variable": (".definitions.variable", "Variable"),
    "WaitForAll": (".definitions.partition_mappers.wait_policy", "WaitForAll"),
    "WeekWindow": (".definitions.partition_mappers.window", "WeekWindow"),
    "WeightRule": (".api.datamodels._generated", "WeightRule"),
    "Window": (".definitions.partition_mappers.window", "Window"),
    "XComArg": (".definitions.xcom_arg", "XComArg"),
    "YearWindow": (".definitions.partition_mappers.window", "YearWindow"),
    "asset": (".definitions.asset.decorators", "asset"),
    "chain": (".bases.operator", "chain"),
    "chain_linear": (".bases.operator", "chain_linear"),
    "conf": (".configuration", "conf"),
    "cross_downstream": (".bases.operator", "cross_downstream"),
    "dag": (".definitions.dag", "dag"),
    "deadline_reference": (".definitions.deadline", "deadline_reference"),
    "NEVER_EXPIRE": (".execution_time.context", "NEVER_EXPIRE"),
    "get_current_context": (".definitions.context", "get_current_context"),
    "get_parsing_context": (".definitions.context", "get_parsing_context"),
    "literal": (".definitions.template", "literal"),
    "lineage": (".lineage", ""),
    "macros": (".execution_time.macros", ""),
    "result": (".definitions.decorators", "result"),
    "setup": (".definitions.decorators", "setup"),
    "task": (".definitions.decorators", "task"),
    "task_group": (".definitions.decorators", "task_group"),
    "teardown": (".definitions.decorators", "teardown"),
}


def __getattr__(name: str):
    module_path, attr_name = __lazy_imports.get(name, ("", ""))
    if not module_path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    mod = importlib.import_module(module_path, __name__)
    val = getattr(mod, attr_name) if attr_name else mod

    # Store for next time
    globals()[name] = val
    return val
