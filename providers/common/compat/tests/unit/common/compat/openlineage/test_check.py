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

import logging
import sys
import types
from importlib import metadata
from unittest.mock import patch

import pytest

from airflow.providers.common.compat.openlineage.check import require_openlineage_version


def _mock_version(package):
    if package == "apache-airflow-providers-openlineage":
        return "1.0.0"
    if package == "openlineage-python":
        return "1.0.0"
    raise Exception("Unexpected package")


def test_decorator_without_arguments():
    with pytest.raises(TypeError) as excinfo:

        @require_openlineage_version  # used without parentheses
        def dummy():
            return "result"

    expected_error = (
        "`require_openlineage_version` decorator must be used with at least one argument: "
        "'provider_min_version' or 'client_min_version', "
        'e.g., @require_openlineage_version(provider_min_version="1.0.0")'
    )
    assert str(excinfo.value) == expected_error


def test_decorator_without_arguments_with_parentheses():
    with pytest.raises(ValueError) as excinfo:

        @require_openlineage_version()
        def dummy():
            return "result"

    expected_error = (
        "`require_openlineage_version` decorator must be used with at least one argument: "
        "'provider_min_version' or 'client_min_version', "
        'e.g., @require_openlineage_version(provider_min_version="1.0.0")'
    )
    assert str(excinfo.value) == expected_error


def test_no_arguments_provided():
    with pytest.raises(ValueError) as excinfo:
        require_openlineage_version()
    expected_error = (
        "`require_openlineage_version` decorator must be used with at least one argument: "
        "'provider_min_version' or 'client_min_version', "
        'e.g., @require_openlineage_version(provider_min_version="1.0.0")'
    )
    assert str(excinfo.value) == expected_error


@pytest.mark.parametrize("provider_min_version", ("1.0.0", "0.9", "0", "0.9.9", "1.0.0.dev0", "1.0.0rc1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_provider_version_sufficient(mock_version, caplog, provider_min_version):
    @require_openlineage_version(provider_min_version=provider_min_version)
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result == "result"
    # No log messages about skipping should be emitted.
    assert "skipping function" not in caplog.text


@pytest.mark.parametrize("provider_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_provider_version_insufficient(mock_version, caplog, provider_min_version):
    @require_openlineage_version(provider_min_version=provider_min_version)
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result is None

    expected_log = (
        f"OpenLineage provider version `1.0.0` is lower than required `{provider_min_version}`, "
        "skipping function `dummy` execution"
    )
    assert expected_log in caplog.text


def test_provider_not_found(caplog):
    def fake_version(package):
        if package == "apache-airflow-providers-openlineage":
            raise metadata.PackageNotFoundError
        raise Exception("Unexpected package")

    with patch("importlib.metadata.version", side_effect=fake_version):
        # Simulate that the fallback import returns a module without __version__
        dummy_module = types.ModuleType("airflow.providers.openlineage")
        with patch.dict(sys.modules, {"airflow.providers.openlineage": dummy_module}):

            @require_openlineage_version(provider_min_version="1.0.0")
            def dummy():
                return "result"

            caplog.set_level(logging.INFO)
            result = dummy()
            assert result is None

            expected_log = (
                "OpenLineage provider not found or has no version, skipping function `dummy` execution"
            )
            assert expected_log in caplog.text


def test_provider_fallback_import(caplog):
    def fake_version(package):
        if package == "apache-airflow-providers-openlineage":
            raise metadata.PackageNotFoundError
        raise Exception("Unexpected package")

    with patch("importlib.metadata.version", side_effect=fake_version):
        # Simulate a module with a sufficient __version__
        dummy_module = types.ModuleType("airflow.providers.openlineage")
        dummy_module.__version__ = "1.2.0"
        with patch.dict(sys.modules, {"airflow.providers.openlineage": dummy_module}):

            @require_openlineage_version(provider_min_version="1.0.0")
            def dummy():
                return "result"

            caplog.set_level(logging.INFO)
            result = dummy()
            assert result == "result"
            assert "skipping function" not in caplog.text


@pytest.mark.parametrize("client_min_version", ("1.0.0", "0.9", "0", "0.9.9", "1.0.0.dev0", "1.0.0rc1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_client_version_sufficient(mock_version, caplog, client_min_version):
    @require_openlineage_version(client_min_version=client_min_version)
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result == "result"
    # No log messages about skipping should be emitted.
    assert "skipping function" not in caplog.text


@pytest.mark.parametrize("client_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_client_version_insufficient(mock_version, caplog, client_min_version):
    @require_openlineage_version(client_min_version=client_min_version)
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result is None

    expected_log = (
        f"OpenLineage client version `1.0.0` is lower than required `{client_min_version}`, "
        "skipping function `dummy` execution"
    )
    assert expected_log in caplog.text


def test_client_version_not_found(caplog):
    def fake_version(package):
        if package == "openlineage-python":
            raise metadata.PackageNotFoundError
        raise Exception("Unexpected package")

    with patch("importlib.metadata.version", side_effect=fake_version):

        @require_openlineage_version(client_min_version="1.0.0")
        def dummy():
            return "result"

        caplog.set_level(logging.INFO)
        result = dummy()
        assert result is None
        expected_log = "OpenLineage client not found, skipping function `dummy` execution"
        assert expected_log in caplog.text


@pytest.mark.parametrize("client_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_client_version_insufficient_when_both_passed(mock_version, caplog, client_min_version):
    @require_openlineage_version(provider_min_version="1.0.0", client_min_version=client_min_version)
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result is None

    expected_log = (
        f"OpenLineage client version `1.0.0` is lower than required `{client_min_version}`, "
        "skipping function `dummy` execution"
    )
    assert expected_log in caplog.text


@pytest.mark.parametrize("provider_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_provider_version_insufficient_when_both_passed(mock_version, caplog, provider_min_version):
    @require_openlineage_version(provider_min_version=provider_min_version, client_min_version="1.0.0")
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result is None

    expected_log = (
        f"OpenLineage provider version `1.0.0` is lower than required `{provider_min_version}`, "
        "skipping function `dummy` execution"
    )
    assert expected_log in caplog.text


@pytest.mark.parametrize("client_min_version", ("1.0.0", "0.9", "0", "0.9.9", "1.0.0.dev0", "1.0.0rc1"))
@pytest.mark.parametrize("provider_min_version", ("1.0.0", "0.9", "0", "0.9.9", "1.0.0.dev0", "1.0.0rc1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_both_versions_sufficient(mock_version, caplog, provider_min_version, client_min_version):
    @require_openlineage_version(
        provider_min_version=provider_min_version, client_min_version=client_min_version
    )
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result == "result"
    assert "skipping function" not in caplog.text


@pytest.mark.parametrize("client_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@pytest.mark.parametrize("provider_min_version", ("1.1.0", "1.0.1.dev0", "1.0.1rc1", "2", "1.1"))
@patch("importlib.metadata.version", side_effect=_mock_version)
def test_both_versions_insufficient(mock_version, caplog, provider_min_version, client_min_version):
    @require_openlineage_version(
        provider_min_version=provider_min_version, client_min_version=client_min_version
    )
    def dummy():
        return "result"

    caplog.set_level(logging.INFO)
    result = dummy()
    assert result is None

    expected_log = (
        f"OpenLineage provider version `1.0.0` is lower than required `{provider_min_version}`, "
        "skipping function `dummy` execution"
    )
    assert expected_log in caplog.text
