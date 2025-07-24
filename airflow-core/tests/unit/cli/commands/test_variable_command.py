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

import json
import os
from contextlib import redirect_stdout
from io import StringIO

import pytest
from sqlalchemy import select

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import variable_command
from airflow.models import Variable
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test


class TestCliVariables:
    @classmethod
    def setup_class(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_variables()

    def teardown_method(self):
        clear_db_variables()

    def test_variables_set(self):
        """Test variable_set command"""
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", "bar"]))
        assert Variable.get("foo") is not None
        with pytest.raises(KeyError):
            Variable.get("foo1")

    def test_variables_set_with_description(self):
        """Test variable_set command with optional description argument"""
        expected_var_desc = "foo_bar_description"
        var_key = "foo"
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", var_key, "bar", "--description", expected_var_desc])
        )

        assert Variable.get(var_key) == "bar"
        with create_session() as session:
            actual_var_desc = session.scalar(select(Variable.description).where(Variable.key == var_key))
            assert actual_var_desc == expected_var_desc

        with pytest.raises(KeyError):
            Variable.get("foo1")

    def test_variables_get(self):
        Variable.set("foo", {"foo": "bar"}, serialize_json=True)

        with redirect_stdout(StringIO()) as stdout:
            variable_command.variables_get(self.parser.parse_args(["variables", "get", "foo"]))
            assert stdout.getvalue() == '{\n  "foo": "bar"\n}\n'

    def test_get_variable_default_value(self):
        with redirect_stdout(StringIO()) as stdout:
            variable_command.variables_get(
                self.parser.parse_args(["variables", "get", "baz", "--default", "bar"])
            )
            assert stdout.getvalue() == "bar\n"

    def test_get_variable_missing_variable(self):
        with pytest.raises(SystemExit):
            variable_command.variables_get(self.parser.parse_args(["variables", "get", "no-existing-VAR"]))

    def test_variables_set_different_types(self):
        """Test storage of various data types"""
        # Set a dict
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "dict", '{"foo": "oops"}'])
        )
        # Set a list
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "list", '["oops"]']))
        # Set str
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "str", "hello string"]))
        # Set int
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "int", "42"]))
        # Set float
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "float", "42.0"]))
        # Set true
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "true", "true"]))
        # Set false
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "false", "false"]))
        # Set none
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "null", "null"]))

        # Export and then import
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", "variables_types.json"])
        )
        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", "variables_types.json"])
        )

        # Assert value
        assert Variable.get("dict", deserialize_json=True) == {"foo": "oops"}
        assert Variable.get("list", deserialize_json=True) == ["oops"]
        assert Variable.get("str") == "hello string"  # cannot json.loads(str)
        assert Variable.get("int", deserialize_json=True) == 42
        assert Variable.get("float", deserialize_json=True) == 42.0
        assert Variable.get("true", deserialize_json=True) is True
        assert Variable.get("false", deserialize_json=True) is False
        assert Variable.get("null", deserialize_json=True) is None

        # test variable import skip existing
        # set varliable list to ["airflow"] and have it skip during import
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "list", '["airflow"]']))
        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", "variables_types.json", "--action-on-existing-key", "skip"]
            )
        )
        assert Variable.get("list", deserialize_json=True) == ["airflow"]  # should not be overwritten

        # test variable import fails on existing when action is set to fail
        with pytest.raises(SystemExit):
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", "variables_types.json", "--action-on-existing-key", "fail"]
                )
            )

        os.remove("variables_types.json")

    def test_variables_list(self):
        """Test variable_list command"""
        # Test command is received
        variable_command.variables_list(self.parser.parse_args(["variables", "list"]))

    def test_variables_delete(self):
        """Test variable_delete command"""
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", "bar"]))
        variable_command.variables_delete(self.parser.parse_args(["variables", "delete", "foo"]))
        with pytest.raises(KeyError):
            Variable.get("foo")

    def test_variables_import(self):
        """Test variables_import command"""
        with pytest.raises(SystemExit, match=r"Unsupported file format"):
            variable_command.variables_import(self.parser.parse_args(["variables", "import", os.devnull]))

    def test_variables_export(self):
        """Test variables_export command"""
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.devnull]))

    def test_variables_isolation(self, tmp_path):
        """Test isolation of variables"""
        path1 = tmp_path / "testfile1.json"
        path2 = tmp_path / "testfile2.json"

        # First export
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", '{"foo":"bar"}']))
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "bar", "original"]))
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.fspath(path1)]))

        variable_command.variables_set(self.parser.parse_args(["variables", "set", "bar", "updated"]))
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", '{"foo":"oops"}']))
        variable_command.variables_delete(self.parser.parse_args(["variables", "delete", "foo"]))
        variable_command.variables_import(self.parser.parse_args(["variables", "import", os.fspath(path1)]))

        assert Variable.get("bar") == "original"
        assert Variable.get("foo") == '{\n  "foo": "bar"\n}'

        # Second export
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.fspath(path2)]))

        assert path1.read_text() == path2.read_text()

    def test_variables_import_and_export_with_description(self, tmp_path):
        """Test variables_import with file-description parameted"""
        variables_types_file = tmp_path / "variables_types.json"
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo", "bar", "--description", "Foo var description"])
        )
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo1", "bar1", "--description", "12"])
        )
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo2", "bar2"]))
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", os.fspath(variables_types_file)])
        )

        with open(variables_types_file) as f:
            exported_vars = json.load(f)
            assert exported_vars == {
                "foo": {
                    "description": "Foo var description",
                    "value": "bar",
                },
                "foo1": {
                    "description": "12",
                    "value": "bar1",
                },
                "foo2": "bar2",
            }

        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(variables_types_file)])
        )

        assert Variable.get("foo") == "bar"
        assert Variable.get("foo1") == "bar1"
        assert Variable.get("foo2") == "bar2"

        with create_session() as session:
            assert (
                session.scalar(select(Variable.description).where(Variable.key == "foo"))
                == "Foo var description"
            )
            assert session.scalar(select(Variable.description).where(Variable.key == "foo1")) == "12"

    def test_variables_import_yaml(self, tmp_path):
        """Test variables_import command with YAML file"""
        variables_yaml_file = tmp_path / "variables.yaml"

        # Create YAML content with simple key-value pairs
        yaml_content = (
            "var_string: hello world\n"
            "var_int: 42\n"
            "var_float: 3.14\n"
            "var_bool: true\n"
            'var_json_list: \'["item1", "item2"]\'\n'
            'var_json_dict: \'{"key1": "value1", "key2": "value2"}\'\n'
            "var_with_description:\n"
            "  value: test_value\n"
            '  description: "Variable with description"\n'
        )
        variables_yaml_file.write_text(yaml_content)

        # Import variables from YAML
        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(variables_yaml_file)])
        )

        # Verify imported variables
        assert Variable.get("var_string") == "hello world"
        assert Variable.get("var_int") == "42"  # YAML scalars are converted to strings
        assert Variable.get("var_float") == "3.14"
        assert Variable.get("var_bool") == "true"
        assert Variable.get("var_json_list", deserialize_json=True) == ["item1", "item2"]
        assert Variable.get("var_json_dict", deserialize_json=True) == {"key1": "value1", "key2": "value2"}
        assert Variable.get("var_with_description") == "test_value"

        # Verify description was imported
        with create_session() as session:
            assert (
                session.scalar(select(Variable.description).where(Variable.key == "var_with_description"))
                == "Variable with description"
            )

    def test_variables_import_env(self, tmp_path):
        """Test variables_import command with .env file"""
        variables_env_file = tmp_path / "variables.env"

        # Create ENV content
        env_content = 'VAR_STRING=hello world\nVAR_INT=42\nVAR_JSON={"key": "value", "number": 123}\n'
        variables_env_file.write_text(env_content)

        # Import variables from ENV
        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(variables_env_file)])
        )

        # Verify imported variables
        assert Variable.get("VAR_STRING") == "hello world"
        assert Variable.get("VAR_INT") == "42"  # ENV values are strings
        assert Variable.get("VAR_JSON") == '{"key": "value", "number": 123}'

    def test_variables_import_invalid_format(self, tmp_path):
        """Test variables_import command with invalid file format"""
        invalid_file = tmp_path / "variables.txt"
        invalid_file.write_text("some content")

        # Should raise SystemExit for unsupported format
        with pytest.raises(SystemExit, match=r"Unsupported file format"):
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(invalid_file)])
            )

    def test_variables_import_yaml_with_existing_keys(self, tmp_path):
        """Test variables_import YAML with existing keys and different actions"""
        variables_yaml_file = tmp_path / "variables.yaml"

        # Set up existing variable
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "existing_var", "original"])
        )

        # Create YAML with conflicting key
        yaml_content = "existing_var: updated_value\nnew_var: new_value\n"
        variables_yaml_file.write_text(yaml_content)

        # Test skip action
        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", os.fspath(variables_yaml_file), "--action-on-existing-key", "skip"]
            )
        )

        # Existing variable should not be updated, new variable should be added
        assert Variable.get("existing_var") == "original"
        assert Variable.get("new_var") == "new_value"

        # Test overwrite action
        variable_command.variables_import(
            self.parser.parse_args(
                [
                    "variables",
                    "import",
                    os.fspath(variables_yaml_file),
                    "--action-on-existing-key",
                    "overwrite",
                ]
            )
        )

        # Existing variable should be updated
        assert Variable.get("existing_var") == "updated_value"

        # Test fail action
        with pytest.raises(SystemExit, match=r"already exists"):
            variable_command.variables_import(
                self.parser.parse_args(
                    [
                        "variables",
                        "import",
                        os.fspath(variables_yaml_file),
                        "--action-on-existing-key",
                        "fail",
                    ]
                )
            )
