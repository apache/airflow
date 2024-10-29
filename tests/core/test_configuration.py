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
from __future__ import annotations

import copy
import datetime
import os
import re
import textwrap
import warnings
from io import StringIO
from unittest import mock
from unittest.mock import patch

import pytest

from airflow import configuration
from airflow.configuration import (
    AirflowConfigException,
    AirflowConfigParser,
    conf,
    expand_env_var,
    get_airflow_config,
    get_airflow_home,
    get_all_expansion_variables,
    run_command,
    write_default_airflow_configuration_if_needed,
)
from airflow.providers_manager import ProvidersManager

from tests.utils.test_config import (
    remove_all_configurations,
    set_deprecated_options,
    set_sensitive_config_values,
    use_config,
)
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.reset_warning_registry import reset_warning_registry

HOME_DIR = os.path.expanduser("~")

# The conf has been updated with sql_alchemy_con and deactivate_stale_dags_interval to test the
# functionality of deprecated options support.
conf.deprecated_options[("database", "sql_alchemy_conn")] = ("core", "sql_alchemy_conn", "2.3.0")
conf.deprecated_options[("scheduler", "parsing_cleanup_interval")] = (
    "scheduler",
    "deactivate_stale_dags_interval",
    "2.5.0",
)


@pytest.fixture(scope="module", autouse=True)
def restore_env():
    with mock.patch.dict("os.environ"):
        yield


def parameterized_config(template) -> str:
    """
    Generates configuration from provided template & variables defined in current scope.

    :param template: a config content templated with {{variables}}
    """
    all_vars = get_all_expansion_variables()
    return template.format(**all_vars)


@mock.patch.dict(
    "os.environ",
    {
        "AIRFLOW__TESTSECTION__TESTKEY": "testvalue",
        "AIRFLOW__CORE__FERNET_KEY": "testvalue",
        "AIRFLOW__TESTSECTION__TESTPERCENT": "with%percent",
        "AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD": 'echo -n "OK"',
        "AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD": 'echo -n "NOT OK"',
        # also set minimum conf values required to pass validation
        "AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY": "16",
        "AIRFLOW__CORE__PARALLELISM": "32",
    },
)
class TestConf:
    def test_airflow_home_default(self):
        with mock.patch.dict("os.environ"):
            if "AIRFLOW_HOME" in os.environ:
                del os.environ["AIRFLOW_HOME"]
            assert get_airflow_home() == expand_env_var("~/airflow")

    def test_airflow_home_override(self):
        with mock.patch.dict("os.environ", AIRFLOW_HOME="/path/to/airflow"):
            assert get_airflow_home() == "/path/to/airflow"

    def test_airflow_config_default(self):
        with mock.patch.dict("os.environ"):
            if "AIRFLOW_CONFIG" in os.environ:
                del os.environ["AIRFLOW_CONFIG"]
            assert get_airflow_config("/home/airflow") == expand_env_var("/home/airflow/airflow.cfg")

    def test_airflow_config_override(self):
        with mock.patch.dict("os.environ", AIRFLOW_CONFIG="/path/to/airflow/airflow.cfg"):
            assert get_airflow_config("/home//airflow") == "/path/to/airflow/airflow.cfg"

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_case_sensitivity(self):
        # section and key are case insensitive for get method
        # note: this is not the case for as_dict method
        assert conf.get("core", "percent") == "with%inside"
        assert conf.get("core", "PERCENT") == "with%inside"
        assert conf.get("CORE", "PERCENT") == "with%inside"

    @conf_vars({("core", "key"): "test_value"})
    def test_set_and_get_with_upper_case(self):
        # both get and set should be case insensitive
        assert conf.get("Core", "Key") == "test_value"
        conf.set("Core", "Key", "new_test_value")
        assert conf.get("Core", "Key") == "new_test_value"

    def test_config_as_dict(self):
        """Test that getting config as dict works even if
        environment has non-legal env vars"""
        with mock.patch.dict("os.environ"):
            os.environ["AIRFLOW__VAR__broken"] = "not_ok"
            asdict = conf.as_dict(raw=True, display_sensitive=True)
        assert asdict.get("VAR") is None
        assert asdict["testsection"]["testkey"] == "testvalue"

    def test_env_var_config(self):
        opt = conf.get("testsection", "testkey")
        assert opt == "testvalue"

        opt = conf.get("testsection", "testpercent")
        assert opt == "with%percent"

        assert conf.has_option("testsection", "testkey")

        with mock.patch.dict(
            "os.environ", AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY="nested"
        ):
            opt = conf.get("kubernetes_environment_variables", "AIRFLOW__TESTSECTION__TESTKEY")
            assert opt == "nested"

    @mock.patch.dict(
        "os.environ", AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY="nested"
    )
    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        assert cfg_dict["core"]["unit_test_mode"] == "True"

        assert cfg_dict["core"]["percent"] == "with%inside"

        # test env vars
        assert cfg_dict["testsection"]["testkey"] == "testvalue"
        assert cfg_dict["kubernetes_environment_variables"]["AIRFLOW__TESTSECTION__TESTKEY"] == "nested"

    def test_conf_as_dict_source(self):
        # test display_source
        cfg_dict = conf.as_dict(display_source=True)
        assert cfg_dict["core"]["load_examples"][1] == "airflow.cfg"
        assert cfg_dict["testsection"]["testkey"] == ("testvalue", "env var")
        assert cfg_dict["core"]["fernet_key"] == ("< hidden >", "env var")

    def test_conf_as_dict_sensitive(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True)
        assert cfg_dict["testsection"]["testkey"] == "testvalue"
        assert cfg_dict["testsection"]["testpercent"] == "with%percent"

        # test display_source and display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True, display_source=True)
        assert cfg_dict["testsection"]["testkey"] == ("testvalue", "env var")

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict_raw(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(raw=True, display_sensitive=True)
        assert cfg_dict["testsection"]["testkey"] == "testvalue"

        # Values with '%' in them should be escaped
        assert cfg_dict["testsection"]["testpercent"] == "with%%percent"
        assert cfg_dict["core"]["percent"] == "with%%inside"

    def test_conf_as_dict_exclude_env(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(include_env=False, display_sensitive=True)

        # Since testsection is only created from env vars, it shouldn't be
        # present at all if we don't ask for env vars to be included.
        assert "testsection" not in cfg_dict

    def test_command_precedence(self):
        test_config = """[test]
key1 = hello
key2_cmd = printf cmd_result
key3 = airflow
key4_cmd = printf key4_result
"""
        test_config_default = """[test]
key1 = awesome
key2 = airflow

[another]
key6 = value6
"""

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ("test", "key2"),
            ("test", "key4"),
        }
        assert "hello" == test_conf.get("test", "key1")
        assert "cmd_result" == test_conf.get("test", "key2")
        assert "airflow" == test_conf.get("test", "key3")
        assert "key4_result" == test_conf.get("test", "key4")
        assert "value6" == test_conf.get("another", "key6")

        assert "hello" == test_conf.get("test", "key1", fallback="fb")
        assert "value6" == test_conf.get("another", "key6", fallback="fb")
        assert "fb" == test_conf.get("another", "key7", fallback="fb")
        assert test_conf.getboolean("another", "key8_boolean", fallback="True") is True
        assert 10 == test_conf.getint("another", "key8_int", fallback="10")
        assert 1.0 == test_conf.getfloat("another", "key8_float", fallback="1")

        assert test_conf.has_option("test", "key1")
        assert test_conf.has_option("test", "key2")
        assert test_conf.has_option("test", "key3")
        assert test_conf.has_option("test", "key4")
        assert not test_conf.has_option("test", "key5")
        assert test_conf.has_option("another", "key6")

        cfg_dict = test_conf.as_dict(display_sensitive=True)
        assert "cmd_result" == cfg_dict["test"]["key2"]
        assert "key2_cmd" not in cfg_dict["test"]

        # If we exclude _cmds then we should still see the commands to run, not
        # their values
        cfg_dict = test_conf.as_dict(include_cmds=False, display_sensitive=True)
        assert "key4" not in cfg_dict["test"]
        assert "printf key4_result" == cfg_dict["test"]["key4_cmd"]

    def test_can_read_dot_section(self):
        test_config = """[test.abc]
key1 = true
"""
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        section = "test.abc"
        key = "key1"
        assert test_conf.getboolean(section, key) is True

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__TEST_ABC__KEY1": "false",  # note that the '.' is converted to '_'
            },
        ):
            assert test_conf.getboolean(section, key) is False

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_from_secret_backend(self, mock_hvac):
        """Get Config Value from a Secret Backend"""
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "request_id": "2d48a2ad-6bcb-e5b6-429d-da35fdf31f56",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "data": {
                "data": {"value": "sqlite:////Users/airflow/airflow/airflow.db"},
                "metadata": {
                    "created_time": "2020-03-28T02:10:54.301784Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
            },
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        }

        test_config = """[test]
sql_alchemy_conn_secret = sql_alchemy_conn
"""
        test_config_default = """[test]
sql_alchemy_conn = airflow
"""

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ("test", "sql_alchemy_conn"),
        }

        assert "sqlite:////Users/airflow/airflow/airflow.db" == test_conf.get("test", "sql_alchemy_conn")

    def test_hidding_of_sensitive_config_values(self):
        test_config = """[test]
                         sql_alchemy_conn_secret = sql_alchemy_conn
                      """
        test_config_default = """[test]
                                 sql_alchemy_conn = airflow
                              """
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ("test", "sql_alchemy_conn"),
        }

        assert "airflow" == test_conf.get("test", "sql_alchemy_conn")
        # Hide sensitive fields
        asdict = test_conf.as_dict(display_sensitive=False)
        assert "< hidden >" == asdict["test"]["sql_alchemy_conn"]
        # If display_sensitive is false, then include_cmd, include_env,include_secrets must all be True
        # This ensures that cmd and secrets env are hidden at the appropriate method and no surprises
        with pytest.raises(ValueError):
            test_conf.as_dict(display_sensitive=False, include_cmds=False)
        # Test that one of include_cmds, include_env, include_secret can be false when display_sensitive
        # is True
        assert test_conf.as_dict(display_sensitive=True, include_cmds=False)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_raise_exception_from_secret_backend_connection_error(self, mock_hvac):
        """Get Config Value from a Secret Backend"""

        mock_client = mock.MagicMock()
        # mock_client.side_effect = AirflowConfigException
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = Exception

        test_config = """[test]
sql_alchemy_conn_secret = sql_alchemy_conn
"""
        test_config_default = """[test]
sql_alchemy_conn = airflow
"""
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ("test", "sql_alchemy_conn"),
        }

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                "Cannot retrieve config from alternative secrets backend. "
                "Make sure it is configured properly and that the Backend "
                "is accessible."
            ),
        ):
            test_conf.get("test", "sql_alchemy_conn")

    def test_getboolean(self):
        """Test AirflowConfigParser.getboolean"""
        test_config = """
[type_validation]
key1 = non_bool_value

[true]
key2 = t
key3 = true
key4 = 1

[false]
key5 = f
key6 = false
key7 = 0

[inline-comment]
key8 = true #123
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to bool. Please check "key1" key in "type_validation" section. '
                'Current value: "non_bool_value".'
            ),
        ):
            test_conf.getboolean("type_validation", "key1")
        assert isinstance(test_conf.getboolean("true", "key3"), bool)
        assert test_conf.getboolean("true", "key2") is True
        assert test_conf.getboolean("true", "key3") is True
        assert test_conf.getboolean("true", "key4") is True
        assert test_conf.getboolean("false", "key5") is False
        assert test_conf.getboolean("false", "key6") is False
        assert test_conf.getboolean("false", "key7") is False
        assert test_conf.getboolean("inline-comment", "key8") is True

    def test_getint(self):
        """Test AirflowConfigParser.getint"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getint("invalid", "key1")
        assert isinstance(test_conf.getint("valid", "key2"), int)
        assert 1 == test_conf.getint("valid", "key2")

    def test_getfloat(self):
        """Test AirflowConfigParser.getfloat"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1.23
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to float. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getfloat("invalid", "key1")
        assert isinstance(test_conf.getfloat("valid", "key2"), float)
        assert 1.23 == test_conf.getfloat("valid", "key2")

    def test_getlist(self):
        """Test AirflowConfigParser.getlist"""
        test_config = """
[empty]
key0 = willbereplacedbymock

[single]
key1 = str

[many]
key2 = one,two,three

[diffdelimiter]
key3 = one;two;three
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        single = test_conf.getlist("single", "key1")
        assert isinstance(single, list)
        assert len(single) == 1
        many = test_conf.getlist("many", "key2")
        assert isinstance(many, list)
        assert len(many) == 3
        semicolon = test_conf.getlist("diffdelimiter", "key3", delimiter=";")
        assert isinstance(semicolon, list)
        assert len(semicolon) == 3
        with patch.object(test_conf, "get", return_value=None):
            with pytest.raises(
                AirflowConfigException, match=re.escape("Failed to convert value None to list.")
            ):
                test_conf.getlist("empty", "key0")
        with patch.object(test_conf, "get", return_value=None):
            with pytest.raises(
                AirflowConfigException, match=re.escape("Failed to convert value None to list.")
            ):
                test_conf.getlist("empty", "key0")

    @pytest.mark.parametrize(
        ("config_str", "expected"),
        [
            pytest.param('{"a": 123}', {"a": 123}, id="dict"),
            pytest.param("[1,2,3]", [1, 2, 3], id="list"),
            pytest.param('"abc"', "abc", id="str"),
            pytest.param("2.1", 2.1, id="num"),
            pytest.param("", None, id="empty"),
        ],
    )
    def test_getjson(self, config_str, expected):
        config = textwrap.dedent(
            f"""
            [test]
            json = {config_str}
        """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getjson("test", "json") == expected

    def test_getjson_empty_with_fallback(self):
        config = textwrap.dedent(
            """
            [test]
            json =
            """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getjson("test", "json", fallback={}) == {}
        assert test_conf.getjson("test", "json") is None

    @pytest.mark.parametrize(
        ("fallback"),
        [
            pytest.param({"a": "b"}, id="dict"),
            # fallback is _NOT_ json parsed, but used verbatim
            pytest.param('{"a": "b"}', id="str"),
            pytest.param(None, id="None"),
        ],
    )
    def test_getjson_fallback(self, fallback):
        test_conf = AirflowConfigParser()

        assert test_conf.getjson("test", "json", fallback=fallback) == fallback

    def test_has_option(self):
        test_config = """[test]
key1 = value1
"""
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        assert test_conf.has_option("test", "key1")
        assert not test_conf.has_option("test", "key_not_exists")
        assert not test_conf.has_option("section_not_exists", "key1")

    def test_remove_option(self):
        test_config = """[test]
key1 = hello
key2 = airflow
"""
        test_config_default = """[test]
key1 = awesome
key2 = airflow
"""

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert "hello" == test_conf.get("test", "key1")
        test_conf.remove_option("test", "key1", remove_default=False)
        assert "awesome" == test_conf.get("test", "key1")

        test_conf.remove_option("test", "key2")
        assert not test_conf.has_option("test", "key2")

    def test_getsection(self):
        test_config = """
[test]
key1 = hello
[new_section]
key = value
"""
        test_config_default = """
[test]
key1 = awesome
key2 = airflow

[testsection]
key3 = value3
"""
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert {"key1": "hello", "key2": "airflow"} == test_conf.getsection("test")
        assert {
            "key3": "value3",
            "testkey": "testvalue",
            "testpercent": "with%percent",
        } == test_conf.getsection("testsection")

        assert {"key": "value"} == test_conf.getsection("new_section")

        assert test_conf.getsection("non_existent_section") is None

    def test_get_section_should_respect_cmd_env_variable(self, tmp_path, monkeypatch):
        cmd_file = tmp_path / "testfile.sh"
        cmd_file.write_text("#!/usr/bin/env bash\necho -n difficult_unpredictable_cat_password\n")
        cmd_file.chmod(0o0555)

        monkeypatch.setenv("AIRFLOW__WEBSERVER__SECRET_KEY_CMD", str(cmd_file))
        content = conf.getsection("webserver")
        assert content["secret_key"] == "difficult_unpredictable_cat_password"

    def test_kubernetes_environment_variables_section(self):
        test_config = """
[kubernetes_environment_variables]
key1 = hello
AIRFLOW_HOME = /root/airflow
"""
        test_config_default = """
[kubernetes_environment_variables]
"""
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert {"key1": "hello", "AIRFLOW_HOME": "/root/airflow"} == test_conf.getsection(
            "kubernetes_environment_variables"
        )

    @pytest.mark.parametrize(
        "key, type",
        [
            ("string_value", int),  # Coercion happens here
            ("only_bool_value", bool),
            ("only_float_value", float),
            ("only_integer_value", int),
            ("only_string_value", str),
        ],
    )
    def test_config_value_types(self, key, type):
        section_dict = conf.getsection("example_section")
        assert isinstance(section_dict[key], type)

    def test_auth_backends_adds_session(self):
        with patch("os.environ", {"AIRFLOW__API__AUTH_BACKEND": None}):
            test_conf = AirflowConfigParser(default_config="")
            # Guarantee we have deprecated settings, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_values = {
                "api": {
                    "auth_backends": (
                        re.compile(r"^airflow\.api\.auth\.backend\.deny_all$|^$"),
                        "airflow.api.auth.backend.session",
                        "3.0",
                    ),
                },
            }
            test_conf.read_dict(
                {"api": {"auth_backends": "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth"}}
            )

            with pytest.warns(FutureWarning):
                test_conf.validate()
                assert (
                    test_conf.get("api", "auth_backends")
                    == "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth,airflow.providers.fab.auth_manager.api.auth.backend.session"
                )

    def test_command_from_env(self):
        test_cmdenv_config = """[testcmdenv]
itsacommand = NOT OK
notacommand = OK
"""
        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.read_string(test_cmdenv_config)
        test_cmdenv_conf.sensitive_config_values.add(("testcmdenv", "itsacommand"))
        with mock.patch.dict("os.environ"):
            # AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD maps to ('testcmdenv', 'itsacommand') in
            # sensitive_config_values and therefore should return 'OK' from the environment variable's
            # echo command, and must not return 'NOT OK' from the configuration
            assert test_cmdenv_conf.get("testcmdenv", "itsacommand") == "OK"
            # AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD maps to no entry in sensitive_config_values and therefore
            # the option should return 'OK' from the configuration, and must not return 'NOT OK' from
            # the environment variable's echo command
            assert test_cmdenv_conf.get("testcmdenv", "notacommand") == "OK"

    @pytest.mark.parametrize("display_sensitive, result", [(True, "OK"), (False, "< hidden >")])
    def test_as_dict_display_sensitivewith_command_from_env(self, display_sensitive, result):
        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.sensitive_config_values.add(("testcmdenv", "itsacommand"))
        with mock.patch.dict("os.environ"):
            asdict = test_cmdenv_conf.as_dict(True, display_sensitive)
            assert asdict["testcmdenv"]["itsacommand"] == (result, "cmd")

    def test_parameterized_config_gen(self):
        config = textwrap.dedent(
            """
            [core]
            dags_folder = {AIRFLOW_HOME}/dags
            sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db
            parallelism = 32
            fernet_key = {FERNET_KEY}
        """
        )

        cfg = parameterized_config(config)

        # making sure some basic building blocks are present:
        assert "[core]" in cfg
        assert "dags_folder" in cfg
        assert "sql_alchemy_conn" in cfg
        assert "fernet_key" in cfg

        # making sure replacement actually happened
        assert "{AIRFLOW_HOME}" not in cfg
        assert "{FERNET_KEY}" not in cfg

    def test_config_use_original_when_original_and_fallback_are_present(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        fernet_key = conf.get("core", "FERNET_KEY")

        with conf_vars({("core", "FERNET_KEY_CMD"): "printf HELLO"}):
            fallback_fernet_key = conf.get("core", "FERNET_KEY")

        assert fernet_key == fallback_fernet_key

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        with conf_vars({("core", "fernet_key"): None}):
            with pytest.raises(AirflowConfigException) as ctx:
                conf.get("core", "FERNET_KEY")

        exception = str(ctx.value)
        message = "section/key [core/fernet_key] not found in config"
        assert message == exception

    def test_config_override_original_when_non_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict("os.environ", {key: value}):
            fernet_key = conf.get("core", "FERNET_KEY")

        assert value == fernet_key

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict("os.environ", {key: value}):
            fernet_key = conf.get("core", "FERNET_KEY")

        assert value == fernet_key

    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__DAGS_FOLDER": "/tmp/test_folder"})
    def test_write_should_respect_env_variable(self):
        parser = AirflowConfigParser()
        with StringIO() as string_file:
            parser.write(string_file)
            content = string_file.getvalue()
        assert "dags_folder = /tmp/test_folder" in content

    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__DAGS_FOLDER": "/tmp/test_folder"})
    def test_write_with_only_defaults_should_not_respect_env_variable(self):
        parser = AirflowConfigParser()
        with StringIO() as string_file:
            parser.write(string_file, only_defaults=True)
            content = string_file.getvalue()
        assert "dags_folder = /tmp/test_folder" not in content

    def test_run_command(self):
        write = r'sys.stdout.buffer.write("\u1000foo".encode("utf8"))'

        cmd = f"import sys; {write}; sys.stdout.flush()"

        assert run_command(f"python -c '{cmd}'") == "\u1000foo"

        assert run_command('echo "foo bar"') == "foo bar\n"
        with pytest.raises(AirflowConfigException):
            run_command('bash -c "exit 1"')

    def test_confirm_unittest_mod(self):
        assert conf.get("core", "unit_test_mode")

    def test_enum_default_task_weight_rule_from_conf(self):
        test_conf = AirflowConfigParser(default_config="")
        test_conf.read_dict({"core": {"default_task_weight_rule": "sidestream"}})
        with pytest.raises(AirflowConfigException) as ctx:
            test_conf.validate()
        exception = str(ctx.value)
        message = (
            "`[core] default_task_weight_rule` should not be 'sidestream'. Possible values: "
            "absolute, downstream, upstream."
        )
        assert message == exception

    def test_enum_logging_levels(self):
        test_conf = AirflowConfigParser(default_config="")
        test_conf.read_dict({"logging": {"logging_level": "XXX"}})
        with pytest.raises(AirflowConfigException) as ctx:
            test_conf.validate()
        exception = str(ctx.value)
        message = (
            "`[logging] logging_level` should not be 'XXX'. Possible values: "
            "CRITICAL, FATAL, ERROR, WARN, WARNING, INFO, DEBUG."
        )
        assert message == exception

    def test_as_dict_works_without_sensitive_cmds(self):
        conf_materialize_cmds = conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)
        conf_maintain_cmds = conf.as_dict(display_sensitive=True, raw=True, include_cmds=False)

        assert "sql_alchemy_conn" in conf_materialize_cmds["database"]
        assert "sql_alchemy_conn_cmd" not in conf_materialize_cmds["database"]

        assert "sql_alchemy_conn" in conf_maintain_cmds["database"]
        assert "sql_alchemy_conn_cmd" not in conf_maintain_cmds["database"]

        assert (
            conf_materialize_cmds["database"]["sql_alchemy_conn"]
            == conf_maintain_cmds["database"]["sql_alchemy_conn"]
        )

    def test_as_dict_respects_sensitive_cmds(self):
        conf_conn = conf["database"]["sql_alchemy_conn"]
        test_conf = copy.deepcopy(conf)
        test_conf.read_string(
            textwrap.dedent(
                """
                [database]
                sql_alchemy_conn_cmd = echo -n my-super-secret-conn
                """
            )
        )

        conf_materialize_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)
        conf_maintain_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=False)

        assert "sql_alchemy_conn" in conf_materialize_cmds["database"]
        assert "sql_alchemy_conn_cmd" not in conf_materialize_cmds["database"]

        if conf_conn == test_conf._default_values["database"]["sql_alchemy_conn"]:
            assert conf_materialize_cmds["database"]["sql_alchemy_conn"] == "my-super-secret-conn"

        assert "sql_alchemy_conn_cmd" in conf_maintain_cmds["database"]
        assert conf_maintain_cmds["database"]["sql_alchemy_conn_cmd"] == "echo -n my-super-secret-conn"

        if conf_conn == test_conf._default_values["database"]["sql_alchemy_conn"]:
            assert "sql_alchemy_conn" not in conf_maintain_cmds["database"]
        else:
            assert "sql_alchemy_conn" in conf_maintain_cmds["database"]
            assert conf_maintain_cmds["database"]["sql_alchemy_conn"] == conf_conn

    @mock.patch.dict(
        "os.environ", {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_as_dict_respects_sensitive_cmds_from_env(self):
        test_conf = copy.deepcopy(conf)
        test_conf.read_string("")

        conf_materialize_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)

        assert "sql_alchemy_conn" in conf_materialize_cmds["database"]
        assert "sql_alchemy_conn_cmd" not in conf_materialize_cmds["database"]

        assert conf_materialize_cmds["database"]["sql_alchemy_conn"] == "postgresql://"

    def test_gettimedelta(self):
        test_config = """
[invalid]
# non-integer value
key1 = str

# fractional value
key2 = 300.99

# too large value for C int
key3 = 999999999999999

[valid]
# negative value
key4 = -1

# zero
key5 = 0

# positive value
key6 = 300

[default]
# Equals to None
key7 =
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key1")

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key2" key in "invalid" section. '
                'Current value: "300.99".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key2")

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                "Failed to convert value to timedelta in `seconds`. "
                "Python int too large to convert to C int. "
                'Please check "key3" key in "invalid" section. Current value: "999999999999999".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key3")

        assert isinstance(test_conf.gettimedelta("valid", "key4"), datetime.timedelta)
        assert test_conf.gettimedelta("valid", "key4") == datetime.timedelta(seconds=-1)
        assert isinstance(test_conf.gettimedelta("valid", "key5"), datetime.timedelta)
        assert test_conf.gettimedelta("valid", "key5") == datetime.timedelta(seconds=0)
        assert isinstance(test_conf.gettimedelta("valid", "key6"), datetime.timedelta)
        assert test_conf.gettimedelta("valid", "key6") == datetime.timedelta(seconds=300)
        assert isinstance(test_conf.gettimedelta("default", "key7"), type(None))
        assert test_conf.gettimedelta("default", "key7") is None


@mock.patch.dict(
    "os.environ",
    {
        # set minimum conf values required to pass validation
        "AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY": "16",
        "AIRFLOW__CORE__PARALLELISM": "32",
    },
)
class TestDeprecatedConf:
    @conf_vars(
        {
            ("celery", "worker_concurrency"): None,
            ("celery", "celeryd_concurrency"): None,
        }
    )
    def test_deprecated_options(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        with set_deprecated_options(
            deprecated_options={("celery", "worker_concurrency"): ("celery", "celeryd_concurrency", "2.0.0")}
        ):
            # Remove it so we are sure we use the right setting
            conf.remove_option("celery", "worker_concurrency")

            with pytest.warns(DeprecationWarning):
                with mock.patch.dict("os.environ", AIRFLOW__CELERY__CELERYD_CONCURRENCY="99"):
                    assert conf.getint("celery", "worker_concurrency") == 99

            with pytest.warns(DeprecationWarning), conf_vars({("celery", "celeryd_concurrency"): "99"}):
                assert conf.getint("celery", "worker_concurrency") == 99

    @conf_vars(
        {
            ("celery", "result_backend"): None,
            ("celery", "celery_result_backend"): None,
            ("celery", "celery_result_backend_cmd"): None,
        }
    )
    def test_deprecated_options_cmd(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        with set_deprecated_options(
            deprecated_options={("celery", "result_backend"): ("celery", "celery_result_backend", "2.0.0")}
        ), set_sensitive_config_values(sensitive_config_values={("celery", "celery_result_backend")}):
            conf.remove_option("celery", "result_backend")
            with conf_vars({("celery", "celery_result_backend_cmd"): "/bin/echo 99"}):
                with pytest.warns(DeprecationWarning):
                    tmp = None
                    if "AIRFLOW__CELERY__RESULT_BACKEND" in os.environ:
                        tmp = os.environ.pop("AIRFLOW__CELERY__RESULT_BACKEND")
                    assert conf.getint("celery", "result_backend") == 99
                    if tmp:
                        os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = tmp

    def test_deprecated_values_from_conf(self):
        test_conf = AirflowConfigParser(
            default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
        )
        # Guarantee we have deprecated settings, so we test the deprecation
        # lookup even if we remove this explicit fallback
        test_conf.deprecated_values = {
            "core": {"hostname_callable": (re.compile(r":"), r".", "2.1")},
        }
        test_conf.read_dict({"core": {"hostname_callable": "airflow.utils.net:getfqdn"}})

        with pytest.warns(FutureWarning):
            test_conf.validate()
            assert test_conf.get("core", "hostname_callable") == "airflow.utils.net.getfqdn"

    @pytest.mark.parametrize(
        "old, new",
        [
            (
                ("core", "sql_alchemy_conn", "postgres+psycopg2://localhost/postgres"),
                ("database", "sql_alchemy_conn", "postgresql://localhost/postgres"),
            ),
        ],
    )
    def test_deprecated_env_vars_upgraded_and_removed(self, old, new):
        test_conf = AirflowConfigParser(
            default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
        )
        old_section, old_key, old_value = old
        new_section, new_key, new_value = new
        old_env_var = test_conf._env_var_name(old_section, old_key)
        new_env_var = test_conf._env_var_name(new_section, new_key)

        with pytest.warns(FutureWarning):
            with mock.patch.dict("os.environ", **{old_env_var: old_value}):
                # Can't start with the new env var existing...
                os.environ.pop(new_env_var, None)

                test_conf.validate()
                assert test_conf.get(new_section, new_key) == new_value
                # We also need to make sure the deprecated env var is removed
                # so that any subprocesses don't use it in place of our updated
                # value.
                assert old_env_var not in os.environ
                # and make sure we track the old value as well, under the new section/key
                assert test_conf.upgraded_values[(new_section, new_key)] == old_value

    @pytest.mark.parametrize(
        "conf_dict",
        [
            {},  # Even if the section is absent from config file, environ still needs replacing.
            {"core": {"hostname_callable": "airflow.utils.net.getfqdn"}},
        ],
    )
    def test_deprecated_values_from_environ(self, conf_dict):
        def make_config():
            test_conf = AirflowConfigParser(
                default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
            )
            # Guarantee we have a deprecated setting, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_values = {
                "core": {"hostname_callable": (re.compile(r":"), r".", "2.1")},
            }
            test_conf.read_dict(conf_dict)
            test_conf.validate()
            return test_conf

        with pytest.warns(FutureWarning):
            with mock.patch.dict("os.environ", AIRFLOW__CORE__HOSTNAME_CALLABLE="airflow.utils.net:getfqdn"):
                test_conf = make_config()
                assert test_conf.get("core", "hostname_callable") == "airflow.utils.net.getfqdn"

        with reset_warning_registry():
            with warnings.catch_warnings(record=True) as warning:
                with mock.patch.dict(
                    "os.environ",
                    AIRFLOW__CORE__HOSTNAME_CALLABLE="CarrierPigeon",
                ):
                    test_conf = make_config()
                    assert test_conf.get("core", "hostname_callable") == "CarrierPigeon"
                    assert [] == warning

    @pytest.mark.parametrize(
        ("conf_dict", "environ", "expected"),
        [
            pytest.param({"old_section": {"val": "old_val"}}, None, "old_val", id="old_config"),
            pytest.param(
                {"old_section": {"val": "old_val"}},
                ("AIRFLOW__OLD_SECTION__VAL", "old_env"),
                "old_env",
                id="old_config_old_env",
            ),
            pytest.param(
                {},
                ("AIRFLOW__OLD_SECTION__VAL", "old_env"),
                "old_env",
                id="old_env",
            ),
            pytest.param(
                {"new_section": {"val": "val2"}},
                ("AIRFLOW__OLD_SECTION__VAL", "old_env"),
                "old_env",
                id="new_config_old_env",
            ),
        ],
    )
    def test_deprecated_sections(self, conf_dict, environ, expected, monkeypatch):
        def make_config():
            test_conf = AirflowConfigParser(
                default_config=textwrap.dedent(
                    """
                    [new_section]
                    val=new
                    """
                )
            )
            # Guarantee we have a deprecated setting, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_sections = {
                "new_section": ("old_section", "2.1"),
            }
            test_conf.read_dict(conf_dict)
            test_conf.validate()
            return test_conf

        if environ:
            monkeypatch.setenv(*environ)

        test_conf = make_config()
        with pytest.warns(
            DeprecationWarning,
            match=r"\[old_section\] has been moved to the val option in \[new_section\].*update your config",
        ):
            # Test when you've _set_ the old value that we warn you need to update your config
            assert test_conf.get("new_section", "val") == expected
        with pytest.warns(
            FutureWarning,
            match=r"\[old_section\] has been renamed to \[new_section\].*update your `conf.get",
        ):
            # Test when you read using the old section you get told to change your `conf.get` call
            assert test_conf.get("old_section", "val") == expected

    def test_deprecated_funcs(self):
        for func in [
            "get",
            "getboolean",
            "getfloat",
            "getint",
            "has_option",
            "remove_option",
            "as_dict",
            "set",
        ]:
            with mock.patch(f"airflow.configuration.conf.{func}") as mock_method:
                with pytest.warns(DeprecationWarning):
                    getattr(configuration, func)()
                mock_method.assert_called_once()

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_config(self, display_source: bool):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=False,
                include_cmds=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("mysql://", "airflow.cfg") if display_source else "mysql://"
            )
            # database should be None because the deprecated value is set in config
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "mysql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_both_env_and_config(self, display_source: bool):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("postgresql://", "env var") if display_source else "postgresql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "postgresql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_both_env_and_config_exclude_env(
        self, display_source: bool
    ):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=False,
                include_cmds=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("mysql://", "airflow.cfg") if display_source else "mysql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "mysql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source, raw=True, display_sensitive=True, include_env=True
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("postgresql://", "env var") if display_source else "postgresql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "postgresql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {}, clear=True)
    def test_conf_as_dict_when_both_conf_and_env_are_empty(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(display_source=display_source, raw=True, display_sensitive=True)
            assert cfg_dict["core"].get("sql_alchemy_conn") is None
            # database should be taken from default because the deprecated value is missing in config
            assert cfg_dict["database"].get("sql_alchemy_conn") == (
                (f"sqlite:///{HOME_DIR}/airflow/airflow.db", "default")
                if display_source
                else f"sqlite:///{HOME_DIR}/airflow/airflow.db"
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == f"sqlite:///{HOME_DIR}/airflow/airflow.db"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_cmd_config(self, display_source: bool):
        with use_config(config="deprecated_cmd.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=True,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("postgresql://", "cmd") if display_source else "postgresql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "postgresql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict(
        "os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_conf_as_dict_when_deprecated_value_in_cmd_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=True,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("postgresql://", "cmd") if display_source else "postgresql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "postgresql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict(
        "os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_conf_as_dict_when_deprecated_value_in_cmd_disabled_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") is None
            assert cfg_dict["database"].get("sql_alchemy_conn") == (
                (f"sqlite:///{HOME_DIR}/airflow/airflow.db", "default")
                if display_source
                else f"sqlite:///{HOME_DIR}/airflow/airflow.db"
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == f"sqlite:///{HOME_DIR}/airflow/airflow.db"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_cmd_disabled_config(self, display_source: bool):
        with use_config(config="deprecated_cmd.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") is None
            assert cfg_dict["database"].get("sql_alchemy_conn") == (
                (f"sqlite:///{HOME_DIR}/airflow/airflow.db", "default")
                if display_source
                else f"sqlite:///{HOME_DIR}/airflow/airflow.db"
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == f"sqlite:///{HOME_DIR}/airflow/airflow.db"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_SECRET": "secret_path'"}, clear=True)
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    def test_conf_as_dict_when_deprecated_value_in_secrets(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=True,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") == (
                ("postgresql://", "secret") if display_source else "postgresql://"
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict["database"].get("sql_alchemy_conn") is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == "postgresql://"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_SECRET": "secret_path'"}, clear=True)
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    def test_conf_as_dict_when_deprecated_value_in_secrets_disabled_env(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") is None
            assert cfg_dict["database"].get("sql_alchemy_conn") == (
                (f"sqlite:///{HOME_DIR}/airflow/airflow.db", "default")
                if display_source
                else f"sqlite:///{HOME_DIR}/airflow/airflow.db"
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == f"sqlite:///{HOME_DIR}/airflow/airflow.db"

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    @mock.patch.dict("os.environ", {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_secrets_disabled_config(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="deprecated_secret.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=False,
            )
            assert cfg_dict["core"].get("sql_alchemy_conn") is None
            assert cfg_dict["database"].get("sql_alchemy_conn") == (
                (f"sqlite:///{HOME_DIR}/airflow/airflow.db", "default")
                if display_source
                else f"sqlite:///{HOME_DIR}/airflow/airflow.db"
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get("database", "sql_alchemy_conn") == f"sqlite:///{HOME_DIR}/airflow/airflow.db"

    def test_as_dict_should_not_falsely_emit_future_warning(self):
        from airflow.configuration import AirflowConfigParser

        test_conf = AirflowConfigParser()
        test_conf.read_dict({"scheduler": {"deactivate_stale_dags_interval": 60}})

        with warnings.catch_warnings(record=True) as captured:
            test_conf.as_dict()
        for w in captured:  # only one expected
            assert "deactivate_stale_dags_interval option in [scheduler] has been renamed" in str(w.message)

    def test_suppress_future_warnings_no_future_warning(self):
        from airflow.configuration import AirflowConfigParser

        test_conf = AirflowConfigParser()
        test_conf.read_dict({"scheduler": {"deactivate_stale_dags_interval": 60}})
        with warnings.catch_warnings(record=True) as captured:
            test_conf.items("scheduler")
        assert len(captured) == 1
        c = captured[0]
        assert c.category is FutureWarning
        assert (
            "you should use[scheduler/parsing_cleanup_interval] "
            "instead. Please update your `conf.get*`" in str(c.message)
        )
        with warnings.catch_warnings(record=True) as captured:
            with test_conf.suppress_future_warnings():
                test_conf.items("scheduler")
        assert len(captured) == 1
        c = captured[0]
        assert c.category is DeprecationWarning
        assert (
            "deactivate_stale_dags_interval option in [scheduler] "
            "has been renamed to parsing_cleanup_interval" in str(c.message)
        )

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("deactivate_stale_dags_interval", id="old"),
            pytest.param("parsing_cleanup_interval", id="new"),
        ],
    )
    def test_future_warning_only_for_code_ref(self, key):
        from airflow.configuration import AirflowConfigParser

        old_val = "deactivate_stale_dags_interval"
        test_conf = AirflowConfigParser()
        test_conf.read_dict({"scheduler": {old_val: 60}})  # config has old value
        with warnings.catch_warnings(record=True) as captured:
            test_conf.get("scheduler", str(key))  # could be old or new value

        w = captured.pop()
        assert "the old setting has been used, but please update" in str(w.message)
        assert w.category is DeprecationWarning
        # only if we use old value, do we also get a warning about code update
        if key == old_val:
            w = captured.pop()
            assert "your `conf.get*` call to use the new name" in str(w.message)
            assert w.category is FutureWarning

    def test_as_dict_raw(self):
        test_conf = AirflowConfigParser()
        raw_dict = test_conf.as_dict(raw=True)
        assert "%%" in raw_dict["logging"]["log_format"]

    def test_as_dict_not_raw(self):
        test_conf = AirflowConfigParser()
        raw_dict = test_conf.as_dict(raw=False)
        assert "%%" not in raw_dict["logging"]["log_format"]

    def test_default_value_raw(self):
        test_conf = AirflowConfigParser()
        log_format = test_conf.get_default_value("logging", "log_format", raw=True)
        assert "%%" in log_format

    def test_default_value_not_raw(self):
        test_conf = AirflowConfigParser()
        log_format = test_conf.get_default_value("logging", "log_format", raw=False)
        assert "%%" not in log_format

    def test_default_value_raw_with_fallback(self):
        test_conf = AirflowConfigParser()
        log_format = test_conf.get_default_value("logging", "missing", fallback="aa %%", raw=True)
        assert "%%" in log_format

    def test_default_value_not_raw_with_fallback(self):
        test_conf = AirflowConfigParser()
        log_format = test_conf.get_default_value("logging", "missing", fallback="aa %%", raw=False)
        # Note that fallback is never interpolated so we expect the value passed as-is
        assert "%%" in log_format

    def test_written_defaults_are_raw_for_defaults(self):
        test_conf = AirflowConfigParser()
        with StringIO() as f:
            test_conf.write(f, only_defaults=True)
            string_written = f.getvalue()
        assert "%%(asctime)s" in string_written

    def test_written_defaults_are_raw_for_non_defaults(self):
        test_conf = AirflowConfigParser()
        with StringIO() as f:
            test_conf.write(f)
            string_written = f.getvalue()
        assert "%%(asctime)s" in string_written

    def test_get_sections_including_defaults(self):
        airflow_cfg = AirflowConfigParser()
        airflow_cfg.remove_all_read_configurations()
        default_sections = airflow_cfg.get_sections_including_defaults()
        assert "core" in default_sections
        assert "test-section" not in default_sections
        airflow_cfg.add_section("test-section")
        airflow_cfg.set("test-section", "test-key", "test-value")
        all_sections_including_defaults = airflow_cfg.get_sections_including_defaults()
        assert "core" in all_sections_including_defaults
        assert "test-section" in all_sections_including_defaults
        airflow_cfg.add_section("core")
        airflow_cfg.set("core", "new-test-key", "test-value")
        all_sections_including_defaults = airflow_cfg.get_sections_including_defaults()
        assert "core" in all_sections_including_defaults
        assert "test-section" in all_sections_including_defaults
        assert sum(1 for section in all_sections_including_defaults if section == "core") == 1

    def test_get_options_including_defaults(self):
        airflow_cfg = AirflowConfigParser()
        airflow_cfg.remove_all_read_configurations()
        default_options = airflow_cfg.get_options_including_defaults("core")
        assert "task_runner" in default_options
        assert "StandardTaskRunner" == airflow_cfg.get("core", "task_runner")
        assert "test-key" not in default_options
        no_options = airflow_cfg.get_options_including_defaults("test-section")
        assert no_options == []
        airflow_cfg.add_section("test-section")
        airflow_cfg.set("test-section", "test-key", "test-value")
        test_section_options = airflow_cfg.get_options_including_defaults("test-section")
        assert "test-key" in test_section_options
        assert "StandardTaskRunner" == airflow_cfg.get("core", "task_runner")
        airflow_cfg.add_section("core")
        airflow_cfg.set("core", "new-test-key", "test-value")
        airflow_cfg.set("core", "task_runner", "test-runner")
        all_core_options_including_defaults = airflow_cfg.get_options_including_defaults("core")
        assert "new-test-key" in all_core_options_including_defaults
        assert "dags_folder" in all_core_options_including_defaults
        assert "test-value" == airflow_cfg.get("core", "new-test-key")
        assert "test-runner" == airflow_cfg.get("core", "task_runner")
        assert sum(1 for option in all_core_options_including_defaults if option == "task_runner") == 1


def test_sensitive_values():
    from airflow.settings import conf

    # this list was hardcoded prior to 2.6.2
    # included here to avoid regression in refactor
    # inclusion of keys ending in "password" or "kwargs" is automated from 2.6.2
    # items not matching this pattern must be added here manually
    sensitive_values = {
        ("database", "sql_alchemy_conn"),
        ("core", "fernet_key"),
        ("core", "internal_api_secret_key"),
        ("webserver", "secret_key"),
        ("secrets", "backend_kwargs"),
        ("sentry", "sentry_dsn"),
        ("database", "sql_alchemy_engine_args"),
        ("core", "sql_alchemy_conn"),
        ("celery_broker_transport_options", "sentinel_kwargs"),
        ("celery", "broker_url"),
        ("celery", "flower_basic_auth"),
        ("celery", "result_backend"),
        ("opensearch", "username"),
        ("opensearch", "password"),
    }
    all_keys = {(s, k) for s, v in conf.configuration_description.items() for k in v.get("options")}
    suspected_sensitive = {(s, k) for (s, k) in all_keys if k.endswith(("password", "kwargs"))}
    exclude_list = {
        ("aws_batch_executor", "submit_job_kwargs"),
        ("kubernetes_executor", "delete_option_kwargs"),
        ("aws_ecs_executor", "run_task_kwargs"),  # Only a constrained set of values, none are sensitive
    }
    suspected_sensitive -= exclude_list
    sensitive_values.update(suspected_sensitive)
    assert sensitive_values == conf.sensitive_config_values


def test_restore_and_reload_provider_configuration():
    from airflow.settings import conf

    assert conf.providers_configuration_loaded is True
    assert conf.get("celery", "celery_app_name") == "airflow.providers.celery.executors.celery_executor"
    conf.restore_core_default_configuration()
    assert conf.providers_configuration_loaded is False
    # built-in pre-2-7 celery executor
    assert conf.get("celery", "celery_app_name") == "airflow.executors.celery_executor"
    conf.load_providers_configuration()
    assert conf.providers_configuration_loaded is True
    assert conf.get("celery", "celery_app_name") == "airflow.providers.celery.executors.celery_executor"


def test_error_when_contributing_to_existing_section():
    from airflow.settings import conf

    with conf.make_sure_configuration_loaded(with_providers=True):
        assert conf.providers_configuration_loaded is True
        assert conf.get("celery", "celery_app_name") == "airflow.providers.celery.executors.celery_executor"
        conf.restore_core_default_configuration()
        assert conf.providers_configuration_loaded is False
        conf.configuration_description["celery"] = {
            "description": "Celery Executor configuration",
            "options": {
                "celery_app_name": {
                    "default": "test",
                }
            },
        }
        conf._default_values.add_section("celery")
        conf._default_values.set("celery", "celery_app_name", "test")
        assert conf.get("celery", "celery_app_name") == "test"
        # patching restoring_core_default_configuration to avoid reloading the defaults
        with patch.object(conf, "restore_core_default_configuration"):
            with pytest.raises(
                AirflowConfigException,
                match="The provider apache-airflow-providers-celery is attempting to contribute "
                "configuration section celery that has already been added before. "
                "The source of it: Airflow's core package",
            ):
                conf.load_providers_configuration()
        assert conf.get("celery", "celery_app_name") == "test"


# Technically it's not a DB test, but we want to make sure it's not interfering with xdist non-db tests
# Because the `_cleanup` method might cause side-effect for parallel-run tests
@pytest.mark.db_test
class TestWriteDefaultAirflowConfigurationIfNeeded:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, tmp_path_factory):
        self.test_airflow_home = tmp_path_factory.mktemp("airflow_home")
        self.test_airflow_config = self.test_airflow_home / "airflow.cfg"
        self.test_non_relative_path = tmp_path_factory.mktemp("other")

        with pytest.MonkeyPatch.context() as monkeypatch_ctx:
            self.monkeypatch = monkeypatch_ctx
            self.patch_airflow_home(self.test_airflow_home)
            self.patch_airflow_config(self.test_airflow_config)
            yield
            # make sure any side effects of "write_default_airflow_configuration_if_needed" are removed
            ProvidersManager()._cleanup()

    def patch_airflow_home(self, airflow_home):
        self.monkeypatch.setattr("airflow.configuration.AIRFLOW_HOME", os.fspath(airflow_home))

    def patch_airflow_config(self, airflow_config):
        self.monkeypatch.setattr("airflow.configuration.AIRFLOW_CONFIG", os.fspath(airflow_config))

    def test_default(self):
        """Test write default config in `${AIRFLOW_HOME}/airflow.cfg`."""
        assert not self.test_airflow_config.exists()
        try:
            write_default_airflow_configuration_if_needed()
            assert self.test_airflow_config.exists()
        finally:
            self.test_airflow_config.unlink()

    @pytest.mark.parametrize(
        "relative_to_airflow_home",
        [
            pytest.param(True, id="relative-to-airflow-home"),
            pytest.param(False, id="non-relative-to-airflow-home"),
        ],
    )
    def test_config_already_created(self, relative_to_airflow_home):
        if relative_to_airflow_home:
            test_airflow_config = self.test_airflow_home / "test-existed-config"
        else:
            test_airflow_config = self.test_non_relative_path / "test-existed-config"

        test_airflow_config.write_text("foo=bar")
        write_default_airflow_configuration_if_needed()
        assert test_airflow_config.read_text() == "foo=bar"

    def test_config_path_relative(self):
        """Test write default config in path relative to ${AIRFLOW_HOME}."""
        test_airflow_config_parent = self.test_airflow_home / "config"
        test_airflow_config = test_airflow_config_parent / "test-airflow.config"
        self.patch_airflow_config(test_airflow_config)

        assert not test_airflow_config_parent.exists()
        assert not test_airflow_config.exists()
        write_default_airflow_configuration_if_needed()
        assert test_airflow_config.exists()

    def test_config_path_non_relative_directory_exists(self):
        """Test write default config in path non-relative to ${AIRFLOW_HOME} and directory exists."""
        test_airflow_config_parent = self.test_non_relative_path
        test_airflow_config = test_airflow_config_parent / "test-airflow.cfg"
        self.patch_airflow_config(test_airflow_config)

        assert test_airflow_config_parent.exists()
        assert not test_airflow_config.exists()
        write_default_airflow_configuration_if_needed()
        assert test_airflow_config.exists()

    def test_config_path_non_relative_directory_not_exists(self):
        """Test raise an error if path to config non-relative to ${AIRFLOW_HOME} and directory not exists."""
        test_airflow_config_parent = self.test_non_relative_path / "config"
        test_airflow_config = test_airflow_config_parent / "test-airflow.cfg"
        self.patch_airflow_config(test_airflow_config)

        assert not test_airflow_config_parent.exists()
        assert not test_airflow_config.exists()
        with pytest.raises(FileNotFoundError, match="not exists and it is not relative to"):
            write_default_airflow_configuration_if_needed()
        assert not test_airflow_config.exists()
        assert not test_airflow_config_parent.exists()

    def test_config_paths_is_directory(self):
        """Test raise an error if AIRFLOW_CONFIG is a directory."""
        test_airflow_config = self.test_airflow_home / "config-dir"
        test_airflow_config.mkdir()
        self.patch_airflow_config(test_airflow_config)

        with pytest.raises(IsADirectoryError, match="configuration file, but got a directory"):
            write_default_airflow_configuration_if_needed()

    @conf_vars({("mysection1", "mykey1"): "supersecret1", ("mysection2", "mykey2"): "supersecret2"})
    @patch.object(
        conf,
        "sensitive_config_values",
        new_callable=lambda: [("mysection1", "mykey1"), ("mysection2", "mykey2")],
    )
    @patch("airflow.utils.log.secrets_masker.mask_secret")
    def test_mask_conf_values(self, mock_mask_secret, mock_sensitive_config_values):
        conf.mask_secrets()

        mock_mask_secret.assert_any_call("supersecret1")
        mock_mask_secret.assert_any_call("supersecret2")

        assert mock_mask_secret.call_count == 2
