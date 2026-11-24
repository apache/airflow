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
from unittest import mock

import pytest

from airflow.sdk import get_parsing_context
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.sdk.definitions.context import _AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME, AirflowParsingContext


@pytest.fixture(autouse=True)
def _isolate_bundle_name():
    # patch.dict also undoes the variable the context manager leaves set, which monkeypatch.delenv
    # would not track when the variable was absent to begin with.
    with mock.patch.dict(os.environ):
        os.environ.pop(_AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME, None)
        yield


def test_bundle_name_defaults_to_none():
    assert get_parsing_context().bundle_name is None
    assert AirflowParsingContext(dag_id="d", task_id="t").bundle_name is None


@pytest.mark.parametrize(
    ("previous", "expected_after"),
    [
        pytest.param(None, "my-bundle", id="stays-set-without-previous-value"),
        pytest.param("outer-bundle", "outer-bundle", id="restores-previous-value"),
    ],
)
def test_context_manager_sets_bundle_name(previous, expected_after):
    if previous is not None:
        os.environ[_AIRFLOW_PARSING_CONTEXT_BUNDLE_NAME] = previous

    with _airflow_parsing_context_manager(bundle_name="my-bundle"):
        assert get_parsing_context().bundle_name == "my-bundle"

    assert get_parsing_context().bundle_name == expected_after
