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
Python task handlers: Python's equivalent to Language SDK task handlers.

A function decorated by ``@task_handler(dag_id, task_id)`` is the Python body of
a stub task.
"""

from __future__ import annotations

from airflow.sdk.task_handlers.binding import call_handler
from airflow.sdk.task_handlers.declaration import (
    TaskHandlerDeclaration,
    TaskHandlerParam,
)
from airflow.sdk.task_handlers.loader import import_handler_modules, resolve_handler_bundle
from airflow.sdk.task_handlers.registry import get_handler, task_handler

__all__ = [
    "import_handler_modules",
    "resolve_handler_bundle",
    "TaskHandlerDeclaration",
    "TaskHandlerParam",
    "call_handler",
    "get_handler",
    "task_handler",
]
