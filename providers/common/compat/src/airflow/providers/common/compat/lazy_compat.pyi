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
"""
Type stubs for IDE autocomplete - always uses Airflow 3 paths.

This file is auto-generated from lazy_compat.py.
    - run scripts/ci/prek/check_common_compat_lazy_imports.py --generate instead.
"""

import airflow.sdk.io as io
import airflow.sdk.timezone as timezone
from airflow.exceptions import (
    AirflowBadRequest as AirflowBadRequest,
    AirflowConfigException as AirflowConfigException,
    AirflowException as AirflowException,
    AirflowFailException as AirflowFailException,
    AirflowNotFoundException as AirflowNotFoundException,
    AirflowSensorTimeout as AirflowSensorTimeout,
    AirflowSkipException as AirflowSkipException,
    AirflowTaskTerminated as AirflowTaskTerminated,
    AirflowTaskTimeout as AirflowTaskTimeout,
)
from airflow.models.dagrun import DagRun as DagRun
from airflow.models.mappedoperator import MappedOperator as MappedOperator
from airflow.models.taskinstance import TaskInstance as TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY as XCOM_RETURN_KEY
from airflow.providers.standard.hooks.filesystem import FSHook as FSHook
from airflow.providers.standard.hooks.package_index import PackageIndexHook as PackageIndexHook
from airflow.providers.standard.hooks.subprocess import SubprocessHook as SubprocessHook
from airflow.providers.standard.operators.bash import BashOperator as BashOperator
from airflow.providers.standard.operators.branch import (
    BaseBranchOperator as BaseBranchOperator,
    BranchMixIn as BranchMixIn,
)
from airflow.providers.standard.operators.datetime import BranchDateTimeOperator as BranchDateTimeOperator
from airflow.providers.standard.operators.empty import EmptyOperator as EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator as LatestOnlyOperator
from airflow.providers.standard.operators.python import (
    _SERIALIZERS as _SERIALIZERS,
    BranchExternalPythonOperator as BranchExternalPythonOperator,
    BranchPythonOperator as BranchPythonOperator,
    BranchPythonVirtualenvOperator as BranchPythonVirtualenvOperator,
    ExternalPythonOperator as ExternalPythonOperator,
    PythonOperator as PythonOperator,
    PythonVirtualenvOperator as PythonVirtualenvOperator,
    ShortCircuitOperator as ShortCircuitOperator,
)
from airflow.providers.standard.operators.smooth import SmoothOperator as SmoothOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator as TriggerDagRunOperator
from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator as BranchDayOfWeekOperator
from airflow.providers.standard.sensors.bash import BashSensor as BashSensor
from airflow.providers.standard.sensors.date_time import (
    DateTimeSensor as DateTimeSensor,
    DateTimeSensorAsync as DateTimeSensorAsync,
)
from airflow.providers.standard.sensors.external_task import (
    ExternalDagLink as ExternalDagLink,
    ExternalTaskMarker as ExternalTaskMarker,
    ExternalTaskSensor as ExternalTaskSensor,
)
from airflow.providers.standard.sensors.filesystem import FileSensor as FileSensor
from airflow.providers.standard.sensors.python import PythonSensor as PythonSensor
from airflow.providers.standard.sensors.time import (
    TimeSensor as TimeSensor,
    TimeSensorAsync as TimeSensorAsync,
)
from airflow.providers.standard.sensors.time_delta import (
    TimeDeltaSensor as TimeDeltaSensor,
    TimeDeltaSensorAsync as TimeDeltaSensorAsync,
)
from airflow.providers.standard.sensors.weekday import DayOfWeekSensor as DayOfWeekSensor
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger as TimeDeltaTrigger
from airflow.providers.standard.utils.python_virtualenv import (
    prepare_virtualenv as prepare_virtualenv,
    write_python_script as write_python_script,
)
from airflow.sdk import (
    DAG as DAG,
    Asset as Asset,
    AssetAlias as AssetAlias,
    AssetAll as AssetAll,
    AssetAny as AssetAny,
    BaseHook as BaseHook,
    BaseNotifier as BaseNotifier,
    BaseOperator as BaseOperator,
    BaseOperatorLink as BaseOperatorLink,
    BaseSensorOperator as BaseSensorOperator,
    Connection as Connection,
    Context as Context,
    DagRunState as DagRunState,
    EdgeModifier as EdgeModifier,
    Label as Label,
    Metadata as Metadata,
    ObjectStoragePath as ObjectStoragePath,
    Param as Param,
    PokeReturnValue as PokeReturnValue,
    TaskGroup as TaskGroup,
    TaskInstanceState as TaskInstanceState,
    TriggerRule as TriggerRule,
    Variable as Variable,
    WeightRule as WeightRule,
    XComArg as XComArg,
    chain as chain,
    chain_linear as chain_linear,
    cross_downstream as cross_downstream,
    dag as dag,
    get_current_context as get_current_context,
    get_parsing_context as get_parsing_context,
    setup as setup,
    task as task,
    task_group as task_group,
    teardown as teardown,
)
from airflow.sdk.bases.decorator import (
    DecoratedMappedOperator as DecoratedMappedOperator,
    DecoratedOperator as DecoratedOperator,
    TaskDecorator as TaskDecorator,
)
from airflow.sdk.bases.sensor import poke_mode_only as poke_mode_only
from airflow.sdk.definitions.template import literal as literal
from airflow.sdk.execution_time.timeout import timeout as timeout
from airflow.sdk.execution_time.xcom import XCom as XCom

__all__: list[str] = [
    "AirflowBadRequest",
    "AirflowConfigException",
    "AirflowException",
    "AirflowFailException",
    "AirflowNotFoundException",
    "AirflowSensorTimeout",
    "AirflowSkipException",
    "AirflowTaskTerminated",
    "AirflowTaskTimeout",
    "Asset",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
    "BaseBranchOperator",
    "BaseHook",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "BashOperator",
    "BashSensor",
    "BranchDateTimeOperator",
    "BranchDayOfWeekOperator",
    "BranchExternalPythonOperator",
    "BranchMixIn",
    "BranchPythonOperator",
    "BranchPythonVirtualenvOperator",
    "Connection",
    "Context",
    "DAG",
    "DagRun",
    "DagRunState",
    "DateTimeSensor",
    "DateTimeSensorAsync",
    "DayOfWeekSensor",
    "DecoratedMappedOperator",
    "DecoratedOperator",
    "EdgeModifier",
    "EmptyOperator",
    "ExternalDagLink",
    "ExternalPythonOperator",
    "ExternalTaskMarker",
    "ExternalTaskSensor",
    "FSHook",
    "FileSensor",
    "Label",
    "LatestOnlyOperator",
    "MappedOperator",
    "Metadata",
    "ObjectStoragePath",
    "PackageIndexHook",
    "Param",
    "PokeReturnValue",
    "PythonOperator",
    "PythonSensor",
    "PythonVirtualenvOperator",
    "ShortCircuitOperator",
    "SmoothOperator",
    "SubprocessHook",
    "TaskDecorator",
    "TaskGroup",
    "TaskInstance",
    "TaskInstanceState",
    "TimeDeltaSensor",
    "TimeDeltaSensorAsync",
    "TimeDeltaTrigger",
    "TimeSensor",
    "TimeSensorAsync",
    "TriggerDagRunOperator",
    "TriggerRule",
    "Variable",
    "WeightRule",
    "XCOM_RETURN_KEY",
    "XCom",
    "XComArg",
    "_SERIALIZERS",
    "chain",
    "chain_linear",
    "cross_downstream",
    "dag",
    "get_current_context",
    "get_parsing_context",
    "io",
    "literal",
    "poke_mode_only",
    "prepare_virtualenv",
    "setup",
    "task",
    "task_group",
    "teardown",
    "timeout",
    "timezone",
    "write_python_script",
]
