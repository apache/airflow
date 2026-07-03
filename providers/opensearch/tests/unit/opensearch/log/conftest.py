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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS


@pytest.fixture(autouse=True)
def _no_implicit_remote_task_log_warning(monkeypatch):
    """
    Suppress the deprecated implicit-registration warning during direct handler construction.

    Mirrors the recommended user pattern of defining ``REMOTE_TASK_LOG`` at module
    scope in the ``[logging] logging_config_class`` module, so
    ``OpensearchTaskHandler.__init__`` does not emit
    ``AirflowProviderDeprecationWarning`` (which the forbidden-warnings plugin
    treats as a test failure).

    On Airflow 2.x the handler does not run the registration branch at all, so the
    fixture is a no-op there (and ``_ActiveLoggingConfig`` does not exist).
    On Airflow 3.0/3.1 the handler reads ``airflow.logging_config.REMOTE_TASK_LOG``.
    On Airflow 3.2+ the handler reads ``_ActiveLoggingConfig.remote_task_log``.
    """
    if not AIRFLOW_V_3_0_PLUS:
        return
    if AIRFLOW_V_3_2_PLUS:
        from airflow.logging_config import _ActiveLoggingConfig

        # raising=False is required because remote_task_log is annotation-only at
        # class scope and may not be initialized in isolated test runs.
        monkeypatch.setattr(_ActiveLoggingConfig, "logging_config_loaded", True, raising=False)
        monkeypatch.setattr(_ActiveLoggingConfig, "remote_task_log", object(), raising=False)
    else:
        import airflow.logging_config

        monkeypatch.setattr(airflow.logging_config, "REMOTE_TASK_LOG", object(), raising=False)
