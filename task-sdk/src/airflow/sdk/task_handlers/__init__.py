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
Python task handlers: the Python peer of the Go/TS/Java ``TaskHandler`` surface (ADR-0011).

A ``@task_handler(dag_id, task_id)`` function is the Python body of a stub task — for the
YAML DAG format, a ``run:`` task on the default (Python) queue; for a `.py` Dag, a
``@task.stub`` task. The graph is owned elsewhere; this contributes only the body.

Contents (see README.md):
  * ``task_handler`` / ``get_registry``  -- authoring + registration (registry.py)
  * ``declare_handler``                  -- parse-time declaration for validation (declaration.py)
  * ``bind_arguments`` / ``call_handler`` -- runtime arg binder (binding.py)

Not yet wired: the execution hook that makes an ``is_stub`` task call a registered handler.
"""

from __future__ import annotations

from airflow.sdk.task_handlers.binding import call_handler
from airflow.sdk.task_handlers.declaration import (
    TaskHandlerDeclaration,
    TaskHandlerParam,
    declare_handler,
    infer_value_schema,
)
from airflow.sdk.task_handlers.registry import TaskHandlerRegistry, get_registry, task_handler

__all__ = [
    "TaskHandlerDeclaration",
    "TaskHandlerParam",
    "TaskHandlerRegistry",
    "call_handler",
    "declare_handler",
    "get_registry",
    "infer_value_schema",
    "task_handler",
]
