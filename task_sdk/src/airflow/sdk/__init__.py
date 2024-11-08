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
    "BaseOperator",
    "DAG",
    "EdgeModifier",
    "Label",
    "TaskGroup",
    "dag",
]

if TYPE_CHECKING:
    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.sdk.definitions.dag import DAG, dag
    from airflow.sdk.definitions.edges import EdgeModifier, Label
    from airflow.sdk.definitions.taskgroup import TaskGroup

__lazy_imports: dict[str, str] = {
    "DAG": ".definitions.dag",
    "dag": ".definitions.dag",
    "BaseOperator": ".definitions.baseoperator",
    "TaskGroup": ".definitions.taskgroup",
    "EdgeModifier": ".definitions.edges",
    "Label": ".definitions.edges",
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
