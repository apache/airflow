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

from airflow.providers.openlineage.conf import (
    _is_true,
    _safe_int_convert,
    config_path,
    custom_extractors,
    dag_state_change_process_pool_size,
    disabled_operators,
    is_disabled,
    is_source_enabled,
    namespace,
    selective_enable,
    transport,
)
from tests.test_utils.config import conf_vars, env_vars

_CONFIG_SECTION = "openlineage"
_VAR_CONFIG_PATH = "OPENLINEAGE_CONFIG"
_CONFIG_OPTION_CONFIG_PATH = "config_path"
_VAR_DISABLE_SOURCE_CODE = "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE"
_CONFIG_OPTION_DISABLE_SOURCE_CODE = "disable_source_code"
_CONFIG_OPTION_DISABLED_FOR_OPERATORS = "disabled_for_operators"
_VAR_EXTRACTORS = "OPENLINEAGE_EXTRACTORS"
_CONFIG_OPTION_EXTRACTORS = "extractors"
_VAR_NAMESPACE = "OPENLINEAGE_NAMESPACE"
_CONFIG_OPTION_NAMESPACE = "namespace"
_CONFIG_OPTION_TRANSPORT = "transport"
_VAR_DISABLED = "OPENLINEAGE_DISABLED"
_CONFIG_OPTION_DISABLED = "disabled"
_VAR_URL = "OPENLINEAGE_URL"
_CONFIG_OPTION_SELECTIVE_ENABLE = "selective_enable"
_CONFIG_OPTION_DAG_STATE_CHANGE_PROCESS_POOL_SIZE = "dag_state_change_process_pool_size"

_BOOL_PARAMS = (
    ("1", True),
    ("t", True),
    ("T", True),
    ("tRuE ", True),
    (" true", True),
    ("TRUE", True),
    ("0", False),
    ("f", False),
    ("F", False),
    (" fAlSe", False),
    ("false ", False),
    ("FALSE", False),
)


@pytest.mark.parametrize(
    ("var_string", "expected"),
    (
        *_BOOL_PARAMS,
        ("some_string", False),
        ("aasd123", False),
        (True, True),
        (False, False),
    ),
)
def test_is_true(var_string, expected):
    assert _is_true(var_string) is expected


@pytest.mark.parametrize(
    "input_value, expected",
    [
        ("123", 123),
        (456, 456),
        ("789", 789),
        (0, 0),
        ("0", 0),
    ],
)
def test_safe_int_convert(input_value, expected):
    assert _safe_int_convert(input_value, default=1) == expected


@pytest.mark.parametrize(
    "input_value, default",
    [
        ("abc", 1),
        ("", 2),
        (None, 3),
        ("123abc", 4),
        ([], 5),
        ("1.2", 6),
    ],
)
def test_safe_int_convert_erroneous_values(input_value, default):
    assert _safe_int_convert(input_value, default) == default


@env_vars({_VAR_CONFIG_PATH: "env_var_path"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): None})
def test_config_path_legacy_env_var_is_used_when_no_conf_option_set():
    assert config_path() == "env_var_path"


@env_vars({_VAR_CONFIG_PATH: "env_var_path"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "config_path"})
def test_config_path_conf_option_has_precedence_over_legacy_env_var():
    assert config_path() == "config_path"


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): ""})
def test_config_path_empty_conf_option():
    assert config_path() == ""


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): None})
def test_config_path_do_not_fail_if_conf_option_missing():
    assert config_path() == ""


@pytest.mark.parametrize(
    ("var_string", "expected"),
    _BOOL_PARAMS,
)
def test_disable_source_code(var_string, expected):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): var_string}):
        result = is_source_enabled()
        assert result is not expected  # conf is disabled_... and func is enabled_... hence the `not` here


@env_vars({_VAR_DISABLE_SOURCE_CODE: "true"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): None})
def test_disable_source_code_legacy_env_var_is_used_when_no_conf_option_set():
    assert is_source_enabled() is False


@env_vars({_VAR_DISABLE_SOURCE_CODE: "false"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): "true"})
def test_disable_source_code_conf_option_has_precedence_over_legacy_env_var():
    assert is_source_enabled() is False


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): "asdadawlaksnd"})
def test_disable_source_code_conf_option_not_working_for_random_string():
    assert is_source_enabled() is True


@env_vars({_VAR_DISABLE_SOURCE_CODE: "asdadawlaksnd"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): None})
def test_disable_source_code_legacy_env_var_not_working_for_random_string():
    assert is_source_enabled() is True


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): ""})
def test_disable_source_code_empty_conf_option():
    assert is_source_enabled() is True


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLE_SOURCE_CODE): None})
def test_disable_source_code_do_not_fail_if_conf_option_missing():
    assert is_source_enabled() is True


@pytest.mark.parametrize(
    ("var_string", "expected"),
    _BOOL_PARAMS,
)
def test_selective_enable(var_string, expected):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_SELECTIVE_ENABLE): var_string}):
        result = selective_enable()
        assert result is expected


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_SELECTIVE_ENABLE): "asdadawlaksnd"})
def test_selective_enable_not_working_for_random_string():
    assert selective_enable() is False


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_SELECTIVE_ENABLE): ""})
def test_selective_enable_empty_conf_option():
    assert selective_enable() is False


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_SELECTIVE_ENABLE): None})
def test_selective_enable_do_not_fail_if_conf_option_missing():
    assert selective_enable() is False


@pytest.mark.parametrize(
    ("var_string", "expected"),
    (
        ("    ", {}),
        ("  ;   ", {}),
        (";", {}),
        ("path.to.Operator  ;", {"path.to.Operator"}),
        ("  ;  path.to.Operator  ;", {"path.to.Operator"}),
        ("path.to.Operator", {"path.to.Operator"}),
        ("path.to.Operator  ;   path.to.second.Operator ; ", {"path.to.Operator", "path.to.second.Operator"}),
        ("path.to.Operator;path.to.second.Operator", {"path.to.Operator", "path.to.second.Operator"}),
    ),
)
def test_disabled_for_operators(var_string, expected):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLED_FOR_OPERATORS): var_string}):
        result = disabled_operators()
        assert isinstance(result, set)
        assert sorted(result) == sorted(expected)


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLED_FOR_OPERATORS): ""})
def test_disabled_for_operators_empty_conf_option():
    assert disabled_operators() == set()


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLED_FOR_OPERATORS): None})
def test_disabled_for_operators_do_not_fail_if_conf_option_missing():
    assert disabled_operators() == set()


@env_vars({_VAR_EXTRACTORS: "path.Extractor"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_EXTRACTORS): None})
def test_extractors_legacy_legacy_env_var_is_used_when_no_conf_option_set():
    assert os.getenv(_VAR_EXTRACTORS) == "path.Extractor"
    assert custom_extractors() == {"path.Extractor"}


@env_vars({_VAR_EXTRACTORS: "env.Extractor"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_EXTRACTORS): "conf.Extractor"})
def test_extractors_conf_option_has_precedence_over_legacy_env_var():
    assert os.getenv(_VAR_EXTRACTORS) == "env.Extractor"
    assert custom_extractors() == {"conf.Extractor"}


@pytest.mark.parametrize(
    ("var_string", "expected"),
    (
        ("path.to.Extractor  ;", {"path.to.Extractor"}),
        ("  ;  path.to.Extractor  ;", {"path.to.Extractor"}),
        ("path.to.Extractor", {"path.to.Extractor"}),
        (
            "path.to.Extractor  ;   path.to.second.Extractor ; ",
            {"path.to.Extractor", "path.to.second.Extractor"},
        ),
        ("path.to.Extractor;path.to.second.Extractor", {"path.to.Extractor", "path.to.second.Extractor"}),
    ),
)
def test_extractors(var_string, expected):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_EXTRACTORS): var_string}):
        result = custom_extractors()
        assert isinstance(result, set)
        assert sorted(result) == sorted(expected)


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_EXTRACTORS): ""})
def test_extractors_empty_conf_option():
    assert custom_extractors() == set()


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_EXTRACTORS): None})
def test_extractors_do_not_fail_if_conf_option_missing():
    assert custom_extractors() == set()


@env_vars({_VAR_NAMESPACE: "my_custom_namespace"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_NAMESPACE): None})
def test_namespace_legacy_env_var_is_used_when_no_conf_option_set():
    assert os.getenv(_VAR_NAMESPACE) == "my_custom_namespace"
    assert namespace() == "my_custom_namespace"


@env_vars({_VAR_NAMESPACE: "env_namespace"})
@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_NAMESPACE): "my_custom_namespace"})
def test_namespace_conf_option_has_precedence_over_legacy_env_var():
    assert os.getenv(_VAR_NAMESPACE) == "env_namespace"
    assert namespace() == "my_custom_namespace"


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_NAMESPACE): ""})
def test_namespace_empty_conf_option():
    assert namespace() == "default"


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_NAMESPACE): None})
def test_namespace_do_not_fail_if_conf_option_missing():
    assert namespace() == "default"


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): '{"valid": "json"}'})
def test_transport_valid():
    assert transport() == {"valid": "json"}


@pytest.mark.parametrize("transport_value", ('["a", "b"]', "[]", '[{"a": "b"}]'))
def test_transport_not_valid(transport_value):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): transport_value}):
        with pytest.raises(ValueError):
            transport()


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): ""})
def test_transport_empty_conf_option():
    assert transport() == {}


@conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): None})
def test_transport_do_not_fail_if_conf_option_missing():
    assert transport() == {}


@pytest.mark.parametrize("disabled", ("1", "t", "T", "true", "TRUE", "True"))
@mock.patch.dict(os.environ, {_VAR_URL: ""}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
    }
)
def test_is_disabled_possible_values_for_disabling(disabled):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): disabled}):
        assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: "https://test.com"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "asdadawlaksnd",
    }
)
def test_is_disabled_is_not_disabled_by_random_string():
    assert is_disabled() is False


@mock.patch.dict(os.environ, {_VAR_URL: "https://test.com"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "",
    }
)
def test_is_disabled_is_false_when_not_explicitly_disabled_and_url_set():
    assert is_disabled() is False


@mock.patch.dict(os.environ, {_VAR_URL: ""}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): '{"valid": "transport"}',
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "",
    }
)
def test_is_disabled_is_false_when_not_explicitly_disabled_and_transport_set():
    assert is_disabled() is False


@mock.patch.dict(os.environ, {_VAR_URL: ""}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "some/path.yml",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "",
    }
)
def test_is_disabled_is_false_when_not_explicitly_disabled_and_config_path_set():
    assert is_disabled() is False


@mock.patch.dict(os.environ, {_VAR_URL: "https://test.com"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "some/path.yml",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): '{"valid": "transport"}',
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "true",
    }
)
def test_is_disabled_conf_option_is_enough_to_disable():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: "https://test.com", _VAR_DISABLED: "true"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "some/path.yml",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): '{"valid": "transport"}',
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "",
    }
)
def test_is_disabled_legacy_env_var_is_enough_to_disable():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: "", _VAR_DISABLED: "true"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): None,
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): None,
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): None,
    }
)
def test_is_disabled_legacy_env_var_is_used_when_no_config():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: "", _VAR_DISABLED: "false"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "some/path.yml",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "true",
    }
)
def test_is_disabled_conf_true_has_precedence_over_env_var_false():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: "", _VAR_DISABLED: "true"}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "some/path.yml",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "false",
    }
)
def test_is_disabled_env_var_true_has_precedence_over_conf_false():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: ""}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): "",
    }
)
def test_is_disabled_empty_conf_option():
    assert is_disabled() is True


@mock.patch.dict(os.environ, {_VAR_URL: ""}, clear=True)
@conf_vars(
    {
        (_CONFIG_SECTION, _CONFIG_OPTION_CONFIG_PATH): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_TRANSPORT): "",
        (_CONFIG_SECTION, _CONFIG_OPTION_DISABLED): None,
    }
)
def test_is_disabled_do_not_fail_if_conf_option_missing():
    assert is_disabled() is True


@pytest.mark.parametrize(
    ("var_string", "expected"),
    (
        ("1", 1),
        ("2   ", 2),
        ("  3", 3),
        ("4.56", 1),  # default
        ("asdf", 1),  # default
        ("true", 1),  # default
        ("false", 1),  # default
        ("None", 1),  # default
        ("", 1),  # default
        (" ", 1),  # default
        (None, 1),  # default
    ),
)
def test_dag_state_change_process_pool_size(var_string, expected):
    with conf_vars({(_CONFIG_SECTION, _CONFIG_OPTION_DAG_STATE_CHANGE_PROCESS_POOL_SIZE): var_string}):
        result = dag_state_change_process_pool_size()
        assert result == expected
