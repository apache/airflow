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
"""Tests for the callback supervisor module."""

from __future__ import annotations

import pytest
import structlog

from airflow.sdk.execution_time.callback_supervisor import execute_callback


def callback_no_args():
    """A simple callback that takes no arguments."""
    return "ok"


def callback_with_kwargs(arg1, arg2):
    """A callback that accepts keyword arguments."""
    return f"{arg1}-{arg2}"


def callback_that_raises():
    """A callback that always raises."""
    raise ValueError("something went wrong")


class CallableClass:
    """A class that returns a callable instance (like BaseNotifier)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, context):
        return "notified"


@pytest.fixture
def log():
    return structlog.get_logger()


class TestExecuteCallback:
    def test_successful_callback_no_args(self, log):
        success, error = execute_callback(f"{__name__}.callback_no_args", {}, log)

        assert success is True
        assert error is None

    def test_successful_callback_with_kwargs(self, log):
        success, error = execute_callback(
            f"{__name__}.callback_with_kwargs", {"arg1": "hello", "arg2": "world"}, log
        )

        assert success is True
        assert error is None

    def test_empty_path_returns_failure(self, log):
        success, error = execute_callback("", {}, log)

        assert success is False
        assert "Callback path not found" in error

    def test_import_error_returns_failure(self, log):
        success, error = execute_callback("nonexistent.module.function", {}, log)

        assert success is False
        assert "ModuleNotFoundError" in error

    def test_execution_error_returns_failure(self, log):
        success, error = execute_callback(f"{__name__}.callback_that_raises", {}, log)

        assert success is False
        assert "ValueError" in error

    def test_callable_class_pattern(self, log):
        """Test the class-that-returns-callable pattern (like BaseNotifier)."""
        success, error = execute_callback(f"{__name__}.CallableClass", {"msg": "alert"}, log)

        assert success is True
        assert error is None

    def test_attribute_error_for_nonexistent_function(self, log):
        success, error = execute_callback(f"{__name__}.nonexistent_function_xyz", {}, log)

        assert success is False
        assert "AttributeError" in error
