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
import warnings

import pytest

from airflow.sdk.definitions import context as sdk_context
from airflow.utils.deprecation_tools import DeprecatedImportWarning


@pytest.fixture
def utils_context():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecatedImportWarning)
        return importlib.import_module("airflow.utils.context")


@pytest.mark.parametrize(
    ("name", "expected", "target"),
    [
        pytest.param("Context", sdk_context.Context, "airflow.sdk.Context", id="Context"),
        pytest.param(
            "KNOWN_CONTEXT_KEYS",
            sdk_context.KNOWN_CONTEXT_KEYS,
            "airflow.sdk.definitions.context.KNOWN_CONTEXT_KEYS",
            id="KNOWN_CONTEXT_KEYS",
        ),
        pytest.param(
            "context_merge",
            sdk_context.context_merge,
            "airflow.sdk.definitions.context.context_merge",
            id="context_merge",
        ),
    ],
)
def test_deprecated_import_returns_sdk_attribute(utils_context, name, expected, target):
    with pytest.warns(
        DeprecatedImportWarning,
        match=rf"`airflow\.utils\.context\.{name}` attribute is deprecated\. Please use `'{target}'`",
    ):
        assert getattr(utils_context, name) is expected


def test_context_copy_partial_is_not_provided(utils_context):
    with pytest.raises(AttributeError, match="context_copy_partial"):
        getattr(utils_context, "context_copy_partial")
