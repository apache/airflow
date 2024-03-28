#
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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Airflow deprecation utilities tests."""
from __future__ import annotations

from unittest import mock

import pytest

from airflow.deprecation import (
    AirflowDeprecationAdapter,
    AirflowProviderDeprecationWarning,
    deprecated,
    get_callable_full_path,
    validate_release_string,
)
from airflow.models.dag import DagRun, get_last_dagrun


class _ExampleClass:
    pass


def _example_function():
    pass


@pytest.mark.parametrize(
    "wrapped,expected_entity_type",
    [
        (_ExampleClass, "class"),
        (_example_function, "function (or method)"),
    ],
)
def test_adapter_get_deprecated_msg_entity_type(wrapped, expected_entity_type):
    msg = AirflowDeprecationAdapter().get_deprecated_msg(wrapped, None)
    assert msg.startswith(f"The {expected_entity_type}")


def test_get_deprecated_msg_with_all_parameters():
    adapter = AirflowDeprecationAdapter(
        deprecated_release="1.0.0",
        removal_release="2.0.0",
        use_instead="new_function",
        instructions="with `new_param=True`.",
    )
    msg = adapter.get_deprecated_msg(_example_function, None)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is deprecated "
        "since version `1.0.0` and it will be removed in version `2.0.0`. "
        "Consider using `new_function` instead with `new_param=True`."
    )
    assert msg == expected_msg


def test_get_deprecated_msg_without_deprecated_release():
    adapter = AirflowDeprecationAdapter(
        removal_release="2.0.0", use_instead="new_function", instructions="with `new_param=True`."
    )
    msg = adapter.get_deprecated_msg(_example_function, None)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is "
        "deprecated and it will be removed in version `2.0.0`. Consider using "
        "`new_function` instead with `new_param=True`."
    )
    assert msg == expected_msg


def test_get_deprecated_msg_without_removal_release():
    adapter = AirflowDeprecationAdapter(
        deprecated_release="2.0.0", use_instead="new_function", instructions="with `new_param=True`."
    )
    msg = adapter.get_deprecated_msg(_example_function, None)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is "
        "deprecated since version `2.0.0` and it will be removed in future release. "
        "Consider using `new_function` instead with `new_param=True`."
    )
    assert msg == expected_msg


def test_get_deprecated_msg_instructions_only():
    adapter = AirflowDeprecationAdapter(instructions="Please update your code.")
    msg = adapter.get_deprecated_msg(_example_function, None)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is "
        "deprecated and it will be removed in future release. Please update your code."
    )
    assert msg == expected_msg


def test_get_deprecated_msg_no_extra_info():
    adapter = AirflowDeprecationAdapter()
    msg = adapter.get_deprecated_msg(_example_function, None)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is "
        "deprecated and it will be removed in future release."
    )
    assert msg == expected_msg


def test_get_deprecated_msg_with_category_action_and_instance():
    adapter = AirflowDeprecationAdapter(action="always", category=AirflowProviderDeprecationWarning)
    msg = adapter.get_deprecated_msg(_example_function, _ExampleClass)
    expected_msg = (
        "The function (or method) `tests.core.test_deprecation._example_function` is "
        "deprecated and it will be removed in future release."
    )
    assert msg == expected_msg


def test_get_callable_full_path_function():
    assert get_callable_full_path(get_last_dagrun) == "airflow.models.dag.get_last_dagrun"


def test_get_callable_full_path_method():
    assert get_callable_full_path(DagRun.get_dag) == "airflow.models.dagrun.DagRun.get_dag"


def test_get_callable_full_path_lambda():
    expected = "tests.core.test_deprecation.test_get_callable_full_path_lambda.<locals>.<lambda>"
    assert get_callable_full_path(lambda x: x) == expected


def test_get_callable_full_path_builtin():
    assert get_callable_full_path(len) == "builtins.len"


@pytest.mark.parametrize(("module", "qualname"), ((None, ""), ("", ""), ("", "qualname")))
def test_get_callable_full_path_returns_str_representation_of_callable(module, qualname):
    mocked_callable = mock.Mock()
    mocked_callable.__module__ = module
    mocked_callable.__qualname__ = qualname
    mocked_callable.__str__ = mock.Mock(return_value="str_mock")
    assert get_callable_full_path(mocked_callable) == "str_mock"


def test_get_callable_full_path_returns_correct_full_path():
    mocked_callable = mock.Mock()
    mocked_callable.__module__ = "module"
    mocked_callable.__qualname__ = "qualname"
    assert get_callable_full_path(mocked_callable) == "module.qualname"


def test_validate_release_string_valid():
    validate_release_string("apache-airflow==1.10.12", "test_arg")


def test_validate_release_string_valid_with_provider():
    validate_release_string("apache-airflow-providers-google==2.0.0", "test_arg")


def test_validate_release_string_none():
    validate_release_string(None, "test_arg")


def test_validate_release_string_invalid_format():
    with pytest.raises(ValueError):
        validate_release_string("invalid-format==1.0.0", "test_arg")


def test_validate_release_string_invalid_semver():
    with pytest.raises(ValueError):
        validate_release_string("apache-airflow==1.10", "test_arg")


def test_deprecated_with_no_arguments():
    @deprecated
    class DeprecatedClass:
        pass

    @deprecated
    def deprecated_func():
        pass

    expected_regex = (
        r"^The class `tests\.core\.test_deprecation\.test_deprecated_with_no_arguments"
        r"\.<locals>\.DeprecatedClass` is deprecated and it will be removed in future release.$"
    )
    with pytest.warns(DeprecationWarning, match=expected_regex.format(x="class")):
        DeprecatedClass()

    expected_regex = (
        r"^The function \(or method\) `tests\.core\.test_deprecation"
        r"\.test_deprecated_with_no_arguments\.<locals>\.deprecated_func` is deprecated "
        "and it will be removed in future release.$"
    )
    with pytest.warns(DeprecationWarning, match=expected_regex.format(x="")):
        deprecated_func()


def test_deprecated_with_all_arguments():
    @deprecated(
        use_instead="airflow.models.dagrun.DagRun.fetch_task_instance",
        instructions="with `defferable=True`",
        deprecated_release="apache-airflow==2.7.0",
        removal_release="apache-airflow==2.8.0",
        category=AirflowProviderDeprecationWarning,
    )
    class DeprecatedClass:
        pass

    @deprecated(
        use_instead="airflow.models.dagrun.DagRun.fetch_task_instance",
        instructions="with `defferable=True`",
        deprecated_release="apache-airflow==2.7.0",
        removal_release="apache-airflow-providers-google==2.8.0",
        category=AirflowProviderDeprecationWarning,
    )
    def deprecated_func():
        pass

    expected_regex = (
        r"^The class `tests\.core\.test_deprecation\.test_deprecated_with_all_arguments"
        r"\.<locals>\.DeprecatedClass` "
        r"is deprecated since version `apache-airflow==2.7.0` and it will be removed in version "
        r"`apache-airflow==2.8.0`. Consider using `airflow\.models\.dagrun\.DagRun\.fetch_task_instance` "
        r"instead with `defferable=True`.$"
    )
    with pytest.warns(AirflowProviderDeprecationWarning, match=expected_regex):
        DeprecatedClass()

    expected_regex = (
        r"^The function \(or method\) `tests\.core\.test_deprecation"
        r"\.test_deprecated_with_all_arguments\.<locals>\.deprecated_func` "
        r"is deprecated since version `apache-airflow==2.7.0` and it will be removed in version "
        r"`apache-airflow-providers-google==2.8.0`. Consider using "
        r"`airflow\.models\.dagrun\.DagRun\.fetch_task_instance` "
        r"instead with `defferable=True`.$"
    )
    with pytest.warns(AirflowProviderDeprecationWarning, match=expected_regex):
        deprecated_func()


@pytest.mark.parametrize(
    "release", ("some-lib==2.7.0", "apache-airflow=2.7", "apache-airflow-providers-xxx=2.7", "2.7.0")
)
def test_deprecated_with_wrong_release(release):
    with pytest.raises(ValueError, match="`deprecated_release` must follow the format .*"):

        @deprecated(
            deprecated_release=release,
        )
        class DeprecatedClass:
            pass

    with pytest.raises(ValueError, match="`removal_release` must follow the format .*"):

        @deprecated(
            removal_release=release,
        )
        def deprecated_func():
            pass


@pytest.mark.parametrize("use_instead", ("SqlOperator", "sql.SqlOperator.test_method", "get_hook()"))
def test_deprecated_with_wrong_use_instead_path(use_instead):
    with pytest.raises(ValueError, match="Provide full import path as `use_instead` argument, instead of.*"):

        @deprecated(
            use_instead=use_instead,
        )
        class DeprecatedClass:
            pass

    with pytest.raises(ValueError, match="Provide full import path as `use_instead` argument, instead of.*"):

        @deprecated(
            use_instead=use_instead,
        )
        def deprecated_func():
            pass
