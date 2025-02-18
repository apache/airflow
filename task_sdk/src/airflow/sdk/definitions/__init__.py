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

if TYPE_CHECKING:
    from airflow.sdk.definitions.assets import Asset, AssetAlias, AssetAll, AssetAny  # noqa: TC004
    from airflow.sdk.definitions.assets.decorators import asset  # noqa: TC004
    from airflow.sdk.definitions.assets.metadata import Metadata  # noqa: TC004
    from airflow.sdk.definitions.baseoperator import BaseOperator  # noqa: TC004
    from airflow.sdk.definitions.connection import Connection  # noqa: TC004
    from airflow.sdk.definitions.context import Context, get_current_context  # noqa: TC004
    from airflow.sdk.definitions.dags import DAG, dag  # noqa: TC004
    from airflow.sdk.definitions.edges import EdgeModifier, Label  # noqa: TC004
    from airflow.sdk.definitions.param import Param  # noqa: TC004
    from airflow.sdk.definitions.taskgroup import TaskGroup  # noqa: TC004
    from airflow.sdk.definitions.template import literal  # noqa: TC004
    from airflow.sdk.definitions.variable import Variable  # noqa: TC004
    from airflow.sdk.definitions.xcom_arg import XComArg  # noqa: TC004

__all__ = [
    "Asset",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
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
    "literal",
]

__lazy_imports: dict[str, str] = {
    "Asset": "assets",
    "AssetAlias": "assets",
    "AssetAll": "assets",
    "AssetAny": "assets",
    "BaseOperator": "baseoperator",
    "Connection": "connection",
    "Context": "context",
    "DAG": "dags",
    "EdgeModifier": "edges",
    "Label": "edges",
    "Metadata": "assets.metadata",
    "Param": "param",
    "TaskGroup": "taskgroup",
    "Variable": "variable",
    "XComArg": "xcom_arg",
    "asset": "assets.decorators",
    "dag": "dags",
    "get_current_context": "context",
    "literal": "template",
}


def __getattr__(name: str):
    try:
        module_path = __lazy_imports[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    module = __import__(f"{__name__}.{module_path}")
    globals()[name] = value = getattr(module, name)
    return value
