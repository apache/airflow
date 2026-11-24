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

import os

import pytest

from airflow.sdk import get_parsing_context
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.sdk.definitions.context import _AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME, AirflowParsingContext


@pytest.fixture(autouse=True)
def _clear_bundle_name(monkeypatch):
    monkeypatch.delenv(_AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME, raising=False)


def test_bundle_name_defaults_to_none():
    assert get_parsing_context().bundle_name is None
    assert AirflowParsingContext(dag_id="d", task_id="t").bundle_name is None


@pytest.mark.parametrize("previous", [None, "outer-bundle"])
def test_context_manager_sets_and_restores_bundle_name(monkeypatch, previous):
    if previous is not None:
        monkeypatch.setenv(_AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME, previous)

    with _airflow_parsing_context_manager(bundle_name="my-bundle"):
        assert get_parsing_context().bundle_name == "my-bundle"

    assert os.environ.get(_AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME) == previous


def test_context_manager_restores_bundle_name_on_error():
    with pytest.raises(RuntimeError):
        with _airflow_parsing_context_manager(bundle_name="my-bundle"):
            raise RuntimeError("boom")

    assert _AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME not in os.environ
