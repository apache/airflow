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
    "AssetWatcher",
    "BaseNotifier",
    "BaseOperator",
    "BaseOperatorLink",
    "BaseSensorOperator",
    "Connection",
    "Context",
    "DAG",
    "EdgeModifier",
    "Label",
    "Metadata",
    "ObjectStoragePath",
    "Param",
    "PokeReturnValue",
    "TaskGroup",
    "Variable",
    "XComArg",
    "asset",
    "chain",
    "chain_linear",
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

__version__ = "1.0.6"

if TYPE_CHECKING:
    from airflow.sdk.bases.notifier import BaseNotifier
    from airflow.sdk.bases.operator import BaseOperator, chain, chain_linear, cross_downstream
    from airflow.sdk.bases.operatorlink import BaseOperatorLink
    from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher
    from airflow.sdk.definitions.asset.decorators import asset
    from airflow.sdk.definitions.asset.metadata import Metadata
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context, get_current_context, get_parsing_context
    from airflow.sdk.definitions.dag import DAG, dag
    from airflow.sdk.definitions.decorators import setup, task, teardown
    from airflow.sdk.definitions.decorators.task_group import task_group
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.param import Param
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.definitions.template import literal
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.definitions.xcom_arg import XComArg
    from airflow.sdk.io.path import ObjectStoragePath

__lazy_imports: dict[str, str] = {
    "Asset": ".definitions.asset",
    "AssetAlias": ".definitions.asset",
    "AssetAll": ".definitions.asset",
    "AssetAny": ".definitions.asset",
    "AssetWatcher": ".definitions.asset",
    "BaseNotifier": ".bases.notifier",
    "BaseOperator": ".bases.operator",
    "BaseOperatorLink": ".bases.operatorlink",
    "BaseSensorOperator": ".bases.sensor",
    "Connection": ".definitions.connection",
    "Context": ".definitions.context",
    "DAG": ".definitions.dag",
    "EdgeModifier": ".definitions.edges",
    "Label": ".definitions.edges",
    "Metadata": ".definitions.asset.metadata",
    "ObjectStoragePath": ".io.path",
    "Param": ".definitions.param",
    "PokeReturnValue": ".bases.sensor",
    "TaskGroup": ".definitions.taskgroup",
    "Variable": ".definitions.variable",
    "XComArg": ".definitions.xcom_arg",
    "asset": ".definitions.asset.decorators",
    "chain": ".bases.operator",
    "chain_linear": ".bases.operator",
    "cross_downstream": ".bases.operator",
    "dag": ".definitions.dag",
    "get_current_context": ".definitions.context",
    "get_parsing_context": ".definitions.context",
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
