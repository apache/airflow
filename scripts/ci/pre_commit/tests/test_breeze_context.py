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

"""Tests for breeze context detection."""

from __future__ import annotations

from scripts.ci.agent_skills.breeze_context import get_context


def test_get_context_defaults_to_host(monkeypatch):
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)
    assert get_context() == "host"


def test_get_context_detects_breeze(monkeypatch):
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "1")
    assert get_context() == "breeze"
