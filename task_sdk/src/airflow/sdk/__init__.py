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
    "BaseOperator",
    "Connection",
    "Context",
    "DAG",
    "EdgeModifier",
    "Label",
    "Metadata",
    "Param",
    "TaskGroup",
    "Variable",
    "XComArg",
    "asset",
    "dag",
    "get_current_context",
    "get_parsing_context",
    "literal",
]

__version__ = "1.0.0.alpha1"

if TYPE_CHECKING:
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher
    from airflow.sdk.definitions.assets.decorators import asset
    from airflow.sdk.definitions.assets.metadata import Metadata
    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context, get_current_context, get_parsing_context
    from airflow.sdk.definitions.dag import DAG, dag
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.param import Param
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.definitions.template import literal
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.definitions.xcom_arg import XComArg

__lazy_imports: dict[str, str] = {
    "Asset": ".definitions.asset",
    "AssetAlias": ".definitions.asset",
    "AssetAll": ".definitions.asset",
    "AssetAny": ".definitions.asset",
    "AssetWatcher": ".definitions.asset",
    "BaseOperator": ".definitions.baseoperator",
    "Connection": ".definitions.connection",
    "Context": ".definitions.context",
    "DAG": ".definitions.dag",
    "EdgeModifier": ".definitions.edges",
    "Label": ".definitions.edges",
    "Metadata": ".definitions.asset.metadata",
    "Param": ".definitions.param",
    "TaskGroup": ".definitions.taskgroup",
    "Variable": ".definitions.variable",
    "XComArg": ".definitions.xcom_arg",
    "asset": ".definitions.asset.decorators",
    "dag": ".definitions.dag",
    "get_current_context": ".definitions.context",
    "get_parsing_context": ".definitions.context",
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
