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

import install_airflow_and_providers as m


def test_providers_constraints_mode_is_read_from_providers_env_var(monkeypatch):
    monkeypatch.setenv("DEFAULT_CONSTRAINTS_BRANCH", "constraints-main")
    monkeypatch.setenv("MOUNT_SOURCES", "selected")
    monkeypatch.setenv("AIRFLOW_CONSTRAINTS_MODE", "constraints")
    monkeypatch.setenv("PROVIDERS_CONSTRAINTS_MODE", "constraints-no-providers")

    ctx = m.install_airflow_and_providers.make_context("install_airflow_and_providers", [])

    assert ctx.params["airflow_constraints_mode"] == "constraints"
    assert ctx.params["providers_constraints_mode"] == "constraints-no-providers"
