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

import pytest


@pytest.fixture(autouse=True)
def _no_implicit_remote_task_log_warning(monkeypatch):
    """
    Suppress the deprecated implicit-registration warning during direct handler construction.

    Mirrors the recommended user pattern of defining ``REMOTE_TASK_LOG`` at module
    scope in the ``[logging] logging_config_class`` module, so
    ``ElasticsearchTaskHandler.__init__`` does not emit
    ``AirflowProviderDeprecationWarning`` (which the forbidden-warnings plugin
    treats as a test failure).

    ``raising=False`` is required because ``_ActiveLoggingConfig.remote_task_log``
    is annotation-only at class scope and may not be initialized in isolated test
    runs.
    """
    from airflow.logging_config import _ActiveLoggingConfig

    monkeypatch.setattr(_ActiveLoggingConfig, "logging_config_loaded", True, raising=False)
    monkeypatch.setattr(_ActiveLoggingConfig, "remote_task_log", object(), raising=False)
