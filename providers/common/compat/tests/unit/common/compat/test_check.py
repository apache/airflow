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

from importlib import metadata
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.check import require_provider_version


def test_decorator_usage_without_parentheses():
    with pytest.raises(TypeError):

        @require_provider_version
        def dummy_function():
            pass


def test_empty_provider_name_and_version():
    with pytest.raises(ValueError, match="decorator must be used with two arguments"):

        @require_provider_version("", "")
        def dummy_function():
            pass


def test_invalid_provider_name():
    expected_error = (
        "Full `provider_name` must be provided starting with 'apache-airflow-providers-', "
        "got `invalid-provider-name`."
    )
    with pytest.raises(ValueError, match=expected_error):

        @require_provider_version("invalid-provider-name", "1.0.0")
        def dummy_function():
            pass


@patch("importlib.metadata.version", return_value="0.9.9")
def test_provider_version_lower_than_required(mock_version):
    @require_provider_version("apache-airflow-providers-nonexistingprovidermock", "1.0.0")
    def dummy_function():
        return "Function Executed"

    with pytest.raises(
        AirflowOptionalProviderFeatureException,
        match=r"Provider's `apache-airflow-providers-nonexistingprovidermock` "
        r"version `0.9.9` is lower than required `1.0.0`",
    ):
        dummy_function()


@patch("importlib.import_module", side_effect=ImportError)
@patch("importlib.metadata.version", side_effect=metadata.PackageNotFoundError)
def test_provider_not_installed(mock_version, mock_import):
    @require_provider_version("apache-airflow-providers-nonexistingprovidermock", "1.0.0")
    def dummy_function():
        return "Function Executed"

    with pytest.raises(
        AirflowOptionalProviderFeatureException,
        match=r"Provider `apache-airflow-providers-nonexistingprovidermock` not found or has no version",
    ):
        dummy_function()


@patch("importlib.metadata.version", return_value="2.0.0")
def test_provider_version_ok(mock_version):
    @require_provider_version("apache-airflow-providers-nonexistingprovidermock", "1.0.0")
    def dummy_function():
        return "Function Executed"

    result = dummy_function()
    assert result == "Function Executed"


@patch("importlib.import_module", return_value=type("module", (), {"__version__": "1.5.0"}))
@patch("importlib.metadata.version", side_effect=metadata.PackageNotFoundError)
def test_provider_dynamic_import(mock_version, mock_import):
    @require_provider_version("apache-airflow-providers-nonexistingprovidermock", "1.0.0")
    def dummy_function():
        return "Function Executed"

    result = dummy_function()
    assert result == "Function Executed"
