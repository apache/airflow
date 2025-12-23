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
"""Tests for shared AirflowConfigParser."""

from __future__ import annotations

import datetime
import json
import os
import re
import textwrap
from configparser import ConfigParser
from enum import Enum
from unittest.mock import patch

import pytest

from airflow_shared.configuration.exceptions import AirflowConfigException
from airflow_shared.configuration.parser import AirflowConfigParser as _SharedAirflowConfigParser


class AirflowConfigParser(_SharedAirflowConfigParser):
    """Test parser that extends shared parser for testing."""

    def __init__(self, default_config: str | None = None, *args, **kwargs):
        configuration_description = {
            "test": {
                "options": {
                    "key1": {"default": "default_value"},
                    "key2": {"default": 123},
                }
            }
        }
        _default_values = ConfigParser()
        _default_values.add_section("test")
        _default_values.set("test", "key1", "default_value")
        _default_values.set("test", "key2", "123")
        super().__init__(configuration_description, _default_values, *args, **kwargs)
        self.configuration_description = configuration_description
        self._default_values = _default_values
        self._suppress_future_warnings = False

        if default_config is not None:
            self._update_defaults_from_string(default_config)

    def _update_defaults_from_string(self, config_string: str):
        """Update defaults from string for testing."""
        parser = ConfigParser()
        parser.read_string(config_string)
        for section in parser.sections():
            if section not in self._default_values.sections():
                self._default_values.add_section(section)
            for key, value in parser.items(section):
                self._default_values.set(section, key, value)


class TestAirflowConfigParser:
    """Test the shared AirflowConfigParser parser methods."""

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
        assert test_conf.getint("valid", "key2") == 1

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
        assert test_conf.getfloat("valid", "key2") == 1.23

    def test_getlist(self):
        """Test AirflowConfigParser.getlist"""
        test_config = """
[single]
key1 = str
empty =

[many]
key2 = one,two,three

[diffdelimiter]
key3 = one;two;three
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        single = test_conf.getlist("single", "key1")
        assert single == ["str"]

        empty = test_conf.getlist("single", "empty")
        assert empty == []

        many = test_conf.getlist("many", "key2")
        assert many == ["one", "two", "three"]

        semicolon = test_conf.getlist("diffdelimiter", "key3", delimiter=";")
        assert semicolon == ["one", "two", "three"]

        assert test_conf.getlist("empty", "key0", fallback=None) is None
        assert test_conf.getlist("empty", "key0", fallback=[]) == []

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
        """Test AirflowConfigParser.getjson"""
        config = textwrap.dedent(
            f"""
            [test]
            json = {config_str}
        """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getjson("test", "json") == expected

    def test_getenum(self):
        """Test AirflowConfigParser.getenum"""

        class TestEnum(Enum):
            option1 = 1
            option2 = 2
            option3 = 3
            fallback = 4

        config = """
            [test1]
            option = option1
            [test2]
            option = option2
            [test3]cmd
            option = option3
            [test4]
            option = option4
            """
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getenum("test1", "option", TestEnum) == TestEnum.option1
        assert test_conf.getenum("test2", "option", TestEnum) == TestEnum.option2
        assert test_conf.getenum("test3", "option", TestEnum) == TestEnum.option3
        assert test_conf.getenum("test4", "option", TestEnum, fallback="fallback") == TestEnum.fallback
        with pytest.raises(AirflowConfigException, match=re.escape("option1, option2, option3, fallback")):
            test_conf.getenum("test4", "option", TestEnum)

    def test_getenumlist(self):
        """Test AirflowConfigParser.getenumlist"""

        class TestEnum(Enum):
            option1 = 1
            option2 = 2
            option3 = 3
            fallback = 4

        config = """
            [test1]
            option = option1,option2,option3
            [test2]
            option = option1,option3
            [test3]
            option = option1,option4
            [test4]
            option =
            """
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getenumlist("test1", "option", TestEnum) == [
            TestEnum.option1,
            TestEnum.option2,
            TestEnum.option3,
        ]
        assert test_conf.getenumlist("test2", "option", TestEnum) == [TestEnum.option1, TestEnum.option3]
        assert test_conf.getenumlist("test3", "option", TestEnum) == [TestEnum.option1]
        assert test_conf.getenumlist("test4", "option", TestEnum) == []

    def test_getjson_empty_with_fallback(self):
        """Test AirflowConfigParser.getjson with empty value and fallback"""
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
        """Test AirflowConfigParser.getjson with fallback"""
        test_conf = AirflowConfigParser()

        assert test_conf.getjson("test", "json", fallback=fallback) == fallback

    def test_has_option(self):
        """Test AirflowConfigParser.has_option"""
        test_config = """[test]
key1 = value1
"""
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        assert test_conf.has_option("test", "key1")
        assert not test_conf.has_option("test", "key_not_exists")
        assert not test_conf.has_option("section_not_exists", "key1")

    def test_remove_option(self):
        """Test AirflowConfigParser.remove_option"""
        test_config = """[test]
key1 = hello
key2 = airflow
"""
        test_config_default = """[test]
key1 = awesome
key2 = airflow
"""

        test_conf = AirflowConfigParser(default_config=test_config_default)
        test_conf.read_string(test_config)

        assert test_conf.get("test", "key1") == "hello"
        test_conf.remove_option("test", "key1", remove_default=False)
        assert test_conf.get("test", "key1") == "awesome"

        test_conf.remove_option("test", "key2")
        assert not test_conf.has_option("test", "key2")

    def test_get_with_defaults(self):
        """Test AirflowConfigParser.get() with defaults"""
        test_config_default = """[test]
key1 = default_value
"""
        test_conf = AirflowConfigParser(default_config=test_config_default)
        assert test_conf.get("test", "key1") == "default_value"

    def test_get_mandatory_value(self):
        """Test AirflowConfigParser.get_mandatory_value()"""
        test_conf = AirflowConfigParser()
        test_conf.add_section("test")
        test_conf.set("test", "key1", "value1")

        assert test_conf.get_mandatory_value("test", "key1") == "value1"

        with pytest.raises(
            AirflowConfigException, match=re.escape("section/key [test/missing_key] not found in config")
        ):
            test_conf.get_mandatory_value("test", "missing_key")

    def test_sensitive_config_values(self):
        """Test AirflowConfigParser.sensitive_config_values property"""
        test_conf = AirflowConfigParser()
        test_conf.configuration_description = {
            "test": {
                "options": {
                    "password": {"sensitive": True, "default": "secret"},
                    "api_key": {"sensitive": True, "default": "key123"},
                    "username": {"sensitive": False, "default": "user"},
                    "normal_value": {"default": "value"},
                }
            },
            "database": {
                "options": {
                    "connection": {"sensitive": True, "default": "sqlite://"},
                }
            },
        }
        if "inversed_deprecated_options" in test_conf.__dict__:
            delattr(test_conf, "inversed_deprecated_options")
        if "sensitive_config_values" in test_conf.__dict__:
            delattr(test_conf, "sensitive_config_values")
        sensitive = test_conf.sensitive_config_values
        assert isinstance(sensitive, set)
        assert ("test", "password") in sensitive
        assert ("test", "api_key") in sensitive
        assert ("database", "connection") in sensitive
        assert ("test", "username") not in sensitive
        assert ("test", "normal_value") not in sensitive

    def test_deprecated_options(self):
        """Test AirflowConfigParser handles deprecated options"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = {
                ("new_section", "new_key"): ("old_section", "old_key", "2.0.0"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("old_section")
        test_conf.set("old_section", "old_key", "old_value")

        with pytest.warns(DeprecationWarning, match="old_key"):
            assert test_conf.get("new_section", "new_key", suppress_warnings=True) == "old_value"

        with pytest.raises(AirflowConfigException):
            test_conf.get("new_section", "new_key", lookup_from_deprecated=False, suppress_warnings=True)

    def test_deprecated_options_same_section(self):
        """Test deprecated options in the same section"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = {
                ("test", "new_key"): ("test", "old_key", "2.0.0"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("test")
        test_conf.set("test", "old_key", "old_value")

        with pytest.warns(DeprecationWarning, match="old_key"):
            assert test_conf.get("test", "new_key", suppress_warnings=True) == "old_value"

    def test_deprecated_options_lookup_disabled(self):
        """Test deprecated options with lookup_from_deprecated=False"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = {
                ("new_section", "new_key"): ("old_section", "old_key", "2.0.0"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("old_section")
        test_conf.set("old_section", "old_key", "old_value")

        with pytest.raises(AirflowConfigException):
            test_conf.get("new_section", "new_key", lookup_from_deprecated=False)

    def test_deprecated_options_precedence(self):
        """Test that new option takes precedence over deprecated option"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = {
                ("test", "new_key"): ("test", "old_key", "2.0.0"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("test")
        test_conf.set("test", "old_key", "old_value")
        test_conf.set("test", "new_key", "new_value")
        value = test_conf.get("test", "new_key")
        assert value == "new_value"

    @pytest.mark.parametrize(
        ("deprecated_options_dict", "kwargs", "new_section_expected_value", "old_section_expected_value"),
        [
            pytest.param(
                {("old_section", "old_key"): ("new_section", "new_key", "2.0.0")},
                {"fallback": None},
                None,
                "value",
                id="deprecated_in_different_section_lookup_enabled",
            ),
            pytest.param(
                {("old_section", "old_key"): ("new_section", "new_key", "2.0.0")},
                {"fallback": None, "lookup_from_deprecated": False},
                None,
                None,
                id="deprecated_in_different_section_lookup_disabled",
            ),
            pytest.param(
                {("new_section", "old_key"): ("new_section", "new_key", "2.0.0")},
                {"fallback": None},
                "value",
                None,
                id="deprecated_in_same_section_lookup_enabled",
            ),
            pytest.param(
                {("new_section", "old_key"): ("new_section", "new_key", "2.0.0")},
                {"fallback": None, "lookup_from_deprecated": False},
                None,
                None,
                id="deprecated_in_same_section_lookup_disabled",
            ),
        ],
    )
    def test_deprecated_options_with_lookup_from_deprecated(
        self, deprecated_options_dict, kwargs, new_section_expected_value, old_section_expected_value
    ):
        """Test deprecated options with lookup_from_deprecated parameter"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = deprecated_options_dict.copy()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("new_section")
        test_conf.set("new_section", "new_key", "value")

        if ("new_section", "old_key") in deprecated_options_dict:
            pass

        if ("old_section", "old_key") in deprecated_options_dict:
            test_conf.add_section("old_section")
            test_conf.set("old_section", "old_key", "value")

        result = test_conf.get("new_section", "old_key", **kwargs)
        assert result == new_section_expected_value

        if old_section_expected_value is not None:
            result = test_conf.get("old_section", "old_key", **kwargs)
            assert result == old_section_expected_value

    def test_deprecated_options_cmd(self):
        """Test deprecated options with _cmd suffix"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_options = {
                ("test", "new_key"): ("test", "old_key", "2.0.0"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False
                self.sensitive_config_values = {("test", "old_key")}

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("test")
        test_conf.set("test", "old_key_cmd", 'echo -n "cmd_value"')

        with pytest.warns(DeprecationWarning, match="old_key"):
            assert test_conf.get("test", "new_key", suppress_warnings=False) == "cmd_value"

    def test_cmd_from_env_var(self):
        test_config = textwrap.dedent(
            """\
            [testcmdenv]
            itsacommand=NOT OK
            notacommand=OK
        """
        )
        test_conf = AirflowConfigParser(default_config=test_config)
        test_conf.sensitive_config_values.add(("testcmdenv", "itsacommand"))

        with patch.dict(os.environ, {"AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD": 'echo -n "OK"'}):
            assert test_conf.get("testcmdenv", "itsacommand") == "OK"
            assert test_conf.get("testcmdenv", "notacommand") == "OK"

    def test_cmd_from_config_file(self):
        test_config = textwrap.dedent(
            """\
            [test]
            sensitive_key_cmd=echo -n cmd_value
            non_sensitive_key=config_value
            non_sensitive_key_cmd=echo -n cmd_value
        """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values.add(("test", "sensitive_key"))
        assert test_conf.get("test", "sensitive_key") == "cmd_value"
        assert test_conf.get("test", "non_sensitive_key") == "config_value"

    def test_secret_from_config_file(self):
        class TestParserWithSecretBackend(AirflowConfigParser):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

            def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
                """Mock secrets backend - return a test value"""
                return "secret_value_from_backend"

        test_config = textwrap.dedent(
            """\
            [test]
            sensitive_key_secret=test/secret/path
            non_sensitive_key=config_value
            non_sensitive_key_secret=test/secret/path
        """
        )
        test_conf = TestParserWithSecretBackend()
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values.add(("test", "sensitive_key"))
        assert test_conf.get("test", "sensitive_key") == "secret_value_from_backend"
        assert test_conf.get("test", "non_sensitive_key") == "config_value"

    def test_secret_from_env_var(self):
        class TestParserWithSecretBackend(AirflowConfigParser):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._suppress_future_warnings = False

            def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
                """Mock secrets backend - return a test value"""
                return "secret_value_from_backend"

        test_config = textwrap.dedent(
            """\
            [test]
            sensitive_key=config_value
            non_sensitive_key=config_value
        """
        )
        test_conf = TestParserWithSecretBackend()
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values.add(("test", "sensitive_key"))
        with patch.dict(os.environ, {"AIRFLOW__TEST__SENSITIVE_KEY_SECRET": "test/secret/path"}):
            assert test_conf.get("test", "sensitive_key") == "secret_value_from_backend"
        with patch.dict(os.environ, {"AIRFLOW__TEST__NON_SENSITIVE_KEY_SECRET": "test/secret/path"}):
            assert test_conf.get("test", "non_sensitive_key") == "config_value"

    def test_deprecated_sections(self):
        """Test deprecated sections"""

        class TestParserWithDeprecated(AirflowConfigParser):
            deprecated_sections = {
                "new_section": ("old_section", "2.1"),
            }

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.configuration_description = {}
                self._default_values = ConfigParser()
                self._default_values.add_section("new_section")
                self._default_values.set("new_section", "val", "new")
                self._suppress_future_warnings = False

        test_conf = TestParserWithDeprecated()
        test_conf.add_section("old_section")
        test_conf.set("old_section", "val", "old_val")
        with pytest.warns(DeprecationWarning, match="old_section"):
            assert test_conf.get("new_section", "val", suppress_warnings=True) == "old_val"
        with pytest.warns(FutureWarning, match="old_section"):
            assert test_conf.get("old_section", "val", suppress_warnings=True) == "old_val"

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

    def test_getimport(self):
        test_config = """
[test]
valid_module = json.JSONDecoder
empty_module =
invalid_module = non.existent.module.path
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        result = test_conf.getimport("test", "valid_module")
        assert result is not None
        assert result == json.JSONDecoder

        assert test_conf.getimport("test", "empty_module") is None

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'The object could not be loaded. Please check "invalid_module" key in "test" section. '
                'Current value: "non.existent.module.path".'
            ),
        ):
            test_conf.getimport("test", "invalid_module")

        with pytest.raises(AirflowConfigException):
            test_conf.getimport("test", "missing_module")

    def test_get_mandatory_list_value(self):
        test_config = """
[test]
existing_list = one,two,three
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        result = test_conf.get_mandatory_list_value("test", "existing_list")
        assert result == ["one", "two", "three"]

        with pytest.raises(AirflowConfigException):
            test_conf.get_mandatory_list_value("test", "missing_key")

        with pytest.raises(ValueError, match=r"The value test/missing_key should be set!"):
            test_conf.get_mandatory_list_value("test", "missing_key", fallback=None)

    def test_set_case_insensitive(self):
        # both get and set should be case insensitive
        test_conf = AirflowConfigParser()
        test_conf.add_section("test")
        test_conf.set("test", "key1", "value1")
        test_conf.set("TEST", "KEY1", "value2")
        assert test_conf.get("test", "key1") == "value2"
        assert test_conf.get("TEST", "KEY1") == "value2"
        test_conf.set("Test", "NewKey", "new_value")
        assert test_conf.get("test", "newkey") == "new_value"
        assert test_conf.get("TEST", "NEWKEY") == "new_value"
