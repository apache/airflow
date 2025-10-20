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

This file is auto-generated from sdk.py.
    - run scripts/ci/prek/check_common_compat_lazy_imports.py --generate instead.
"""

import airflow.sdk.io as io
import airflow.sdk.timezone as timezone
from airflow.models.xcom import XCOM_RETURN_KEY as XCOM_RETURN_KEY
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
    "Asset",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
    "BaseHook",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "Connection",
    "Context",
    "DAG",
    "DagRunState",
    "DecoratedMappedOperator",
    "DecoratedOperator",
    "EdgeModifier",
    "Label",
    "Metadata",
    "ObjectStoragePath",
    "Param",
    "PokeReturnValue",
    "TaskDecorator",
    "TaskGroup",
    "TaskInstanceState",
    "TriggerRule",
    "Variable",
    "WeightRule",
    "XCOM_RETURN_KEY",
    "XCom",
    "XComArg",
    "chain",
    "chain_linear",
    "cross_downstream",
    "dag",
    "get_current_context",
    "get_parsing_context",
    "io",
    "literal",
    "poke_mode_only",
    "setup",
    "task",
    "task_group",
    "teardown",
    "timeout",
    "timezone",
]
