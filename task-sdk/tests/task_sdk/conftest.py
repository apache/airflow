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

import sys

import pytest

from airflow.sdk._shared.module_loading.dag_file import UNUSUAL_MODULE_PREFIX
from airflow.sdk.definitions.task_handlers import loader as task_handler_loader
from airflow.sdk.definitions.task_handlers.registry import _REGISTRY


@pytest.fixture
def clean_task_handlers():
    """Reset the process-global handler registry and loader caches around a test.

    Shared by the task-handler test modules, which register handlers and import
    handler bundles into process-global state. Opt in with
    ``pytestmark = pytest.mark.usefixtures("clean_task_handlers")``.
    """

    def _reset():
        _REGISTRY.clear()
        task_handler_loader._imported_bundles.clear()
        task_handler_loader._resolved_bundles.clear()
        for name in [n for n in sys.modules if n.startswith(UNUSUAL_MODULE_PREFIX)]:
            del sys.modules[name]

    saved_path = list(sys.path)
    _reset()
    yield
    added = [p for p in sys.path if p not in saved_path]
    for name, module in list(sys.modules.items()):
        file = getattr(module, "__file__", None)
        if file and any(file.startswith(p) for p in added):
            del sys.modules[name]
    sys.path[:] = saved_path
    _reset()
