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

import importlib
import os
import sys
from unittest import mock


@mock.patch.dict(os.environ, {"AIRFLOW_API_APPS": "core"})
@mock.patch("airflow.api_fastapi.app.cached_app", autospec=True)
@mock.patch("airflow.settings.ensure_initialized", autospec=True)
def test_initializes_before_creating_the_app(mock_ensure_initialized, mock_cached_app, monkeypatch):
    calls = mock.Mock()
    calls.attach_mock(mock_ensure_initialized, "ensure_initialized")
    calls.attach_mock(mock_cached_app, "cached_app")
    monkeypatch.delitem(sys.modules, "airflow.api_fastapi.main", raising=False)

    importlib.import_module("airflow.api_fastapi.main")

    assert calls.mock_calls == [mock.call.ensure_initialized(), mock.call.cached_app(apps="core")]
