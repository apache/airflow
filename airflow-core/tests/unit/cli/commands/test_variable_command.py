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

import pytest
import yaml
from sqlalchemy import select

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import variable_command
from airflow.models import Variable
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test


# Test data fixtures
@pytest.fixture
def simple_variable_data():
    """Simple variables without descriptions for testing"""
    return {
        "key1": "value1",
        "key2": {"nested": "dict", "with": ["list", "values"]},
        "key3": 123,
        "key4": True,
        "key5": None,
    }


@pytest.fixture
def variable_data_with_descriptions():
    """Variables with descriptions for testing"""
    return {
        "var1": {"value": "test_value", "description": "Test description for var1"},
        "var2": {"value": {"complex": "object"}, "description": "Complex variable"},
        "var3": "simple_value",  # No description
    }


@pytest.fixture
def env_variable_data():
    """ENV format test data"""
    return """# Comment line
KEY_A=value_a
KEY_B=value with spaces
KEY_C={"json": "value", "number": 42}

# Another comment
KEY_D=true
KEY_E=123
"""


# Factory fixture for creating variable files
@pytest.fixture
def create_variable_file(tmp_path):
    """Factory to create variable files in different formats"""

    def _create(data, format="yaml", filename=None):
        # Determine filename
        if not filename:
            ext = "yml" if format == "yml" else format
            filename = f"variables.{ext}"

        file_path = tmp_path / filename

        if format in ["yaml", "yml"]:
            with open(file_path, "w") as f:
                yaml.dump(data, f)
        elif format == "json":
            with open(file_path, "w") as f:
                json.dump(data, f)
        elif format == "env":
            # Handle string content directly for ENV format
            if isinstance(data, str):
                file_path.write_text(data)
            else:
                # Convert dict to env format
                lines = []
                for key, value in data.items():
                    if isinstance(value, dict) and "value" in value:
                        actual_value = value["value"]
                    else:
                        actual_value = value

                    if isinstance(actual_value, dict):
                        lines.append(f"{key}={json.dumps(actual_value)}")
                    else:
                        lines.append(f"{key}={actual_value}")
                file_path.write_text("\n".join(lines))

        return file_path

    return _create


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

    def test_variables_get(self, stdout_capture):
        Variable.set("foo", {"foo": "bar"}, serialize_json=True)

        with stdout_capture as stdout:
            variable_command.variables_get(self.parser.parse_args(["variables", "get", "foo"]))
            assert stdout.getvalue() == '{\n  "foo": "bar"\n}\n'

    def test_get_variable_default_value(self, stdout_capture):
        with stdout_capture as stdout:
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
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", "variables_types.json"]), session=session
            )

        # Assert value
        assert Variable.get("dict", deserialize_json=True) == {"foo": "oops"}
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

    @pytest.mark.parametrize(
        "filename",
        [
            os.devnull,  # No extension (special file)
            "variables.txt",  # Unsupported .txt extension
            "variables",  # No extension
            "variables.xml",  # Unsupported .xml extension
        ],
    )
    def test_variables_import_unsupported_format(self, tmp_path, filename):
        """Test variables_import command with unsupported file formats"""
        # Use devnull directly or create a file with unsupported extension
        if filename == os.devnull:
            file_path = filename
        else:
            file_path = tmp_path / filename
            file_path.write_text("some content")
            file_path = os.fspath(file_path)

        with pytest.raises(SystemExit, match=r"Unsupported file format"):
            with create_session() as session:
                variable_command.variables_import(
                    self.parser.parse_args(["variables", "import", file_path]), session=session
                )

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
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(path1)]), session=session
            )

        assert Variable.get("bar") == "original"
        assert Variable.get("foo") == '{\n  "foo": "bar"\n}'

        # Second export
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.fspath(path2)]))

        assert path1.read_text() == path2.read_text()

    def test_variables_import_and_export_with_description(self, tmp_path):
        """Test variables_import with file-description parameter"""
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

        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(variables_types_file)]),
                session=session,
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

    @pytest.mark.parametrize("format", ["json", "yaml", "yml"])
    def test_variables_import_formats(self, create_variable_file, simple_variable_data, format):
        """Test variables_import with different formats (JSON, YAML, YML)"""
        file = create_variable_file(simple_variable_data, format=format)

        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(file)]), session=session
            )

        assert Variable.get("key1") == "value1"
        assert Variable.get("key2", deserialize_json=True) == {"nested": "dict", "with": ["list", "values"]}
        assert Variable.get("key3", deserialize_json=True) == 123
        assert Variable.get("key4", deserialize_json=True) is True
        assert Variable.get("key5", deserialize_json=True) is None

    @pytest.mark.parametrize("format", ["json", "yaml"])
    def test_variables_import_with_descriptions(
        self, create_variable_file, variable_data_with_descriptions, format
    ):
        """Test variables_import with descriptions in different formats (JSON, YAML)"""
        file = create_variable_file(variable_data_with_descriptions, format=format)

        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(file)]), session=session
            )

        assert Variable.get("var1") == "test_value"
        assert Variable.get("var2", deserialize_json=True) == {"complex": "object"}
        assert Variable.get("var3") == "simple_value"

        with create_session() as session:
            assert (
                session.scalar(select(Variable.description).where(Variable.key == "var1"))
                == "Test description for var1"
            )
            assert (
                session.scalar(select(Variable.description).where(Variable.key == "var2"))
                == "Complex variable"
            )
            assert session.scalar(select(Variable.description).where(Variable.key == "var3")) is None

    def test_variables_import_env(self, create_variable_file, env_variable_data):
        """Test variables_import with ENV format"""
        env_file = create_variable_file(env_variable_data, format="env")

        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(env_file)]), session=session
            )

        assert Variable.get("KEY_A") == "value_a"
        assert Variable.get("KEY_B") == "value with spaces"
        assert Variable.get("KEY_C") == '{"json": "value", "number": 42}'
        assert Variable.get("KEY_D") == "true"
        assert Variable.get("KEY_E") == "123"

    @pytest.mark.parametrize("format", ["json", "yaml", "yml"])
    def test_variables_import_action_on_existing(self, create_variable_file, simple_variable_data, format):
        """Test variables_import with action_on_existing_key parameter for different formats"""
        file = create_variable_file(simple_variable_data, format=format)

        # Set up one existing variable with different value
        Variable.set("key1", "original_value")

        # Test skip action - existing key1 should keep original value, others should be imported
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", os.fspath(file), "--action-on-existing-key", "skip"]
                ),
                session=session,
            )
        assert Variable.get("key1") == "original_value"  # Should NOT be overwritten
        assert Variable.get("key2", deserialize_json=True) == {"nested": "dict", "with": ["list", "values"]}
        assert Variable.get("key3", deserialize_json=True) == 123

        # Clean up non-existing keys for next test
        for key in ["key2", "key3", "key4", "key5"]:
            Variable.delete(key)

        # Test overwrite action (default) - existing key1 should be overwritten
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", os.fspath(file), "--action-on-existing-key", "overwrite"]
                ),
                session=session,
            )
        assert Variable.get("key1") == "value1"  # Should be overwritten with new value
        assert Variable.get("key2", deserialize_json=True) == {"nested": "dict", "with": ["list", "values"]}

        # Test fail action - should fail when key1 already exists
        Variable.set("key1", "original_value")
        with pytest.raises(SystemExit, match="already exists"):
            with create_session() as session:
                variable_command.variables_import(
                    self.parser.parse_args(
                        ["variables", "import", os.fspath(file), "--action-on-existing-key", "fail"]
                    ),
                    session=session,
                )

    def test_variables_import_env_action_on_existing(self, tmp_path):
        """Test variables_import ENV with action_on_existing_key parameter"""
        env_file = tmp_path / "variables_update.env"
        env_content = """EXISTING_VAR=updated_value
NEW_VAR=fresh_value"""
        env_file.write_text(env_content)

        # Set up existing variable
        Variable.set("EXISTING_VAR", "initial_value")

        # Test skip action
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", os.fspath(env_file), "--action-on-existing-key", "skip"]
                ),
                session=session,
            )
        assert Variable.get("EXISTING_VAR") == "initial_value"
        assert Variable.get("NEW_VAR") == "fresh_value"

        # Clean up for next test
        Variable.delete("NEW_VAR")

        # Test overwrite action
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", os.fspath(env_file), "--action-on-existing-key", "overwrite"]
                ),
                session=session,
            )
        assert Variable.get("EXISTING_VAR") == "updated_value"
        assert Variable.get("NEW_VAR") == "fresh_value"

    @pytest.mark.parametrize(
        ("format", "invalid_content", "error_pattern"),
        [
            ("json", '{"invalid": "json", missing_quotes: true}', "Failed to load the secret file"),
            ("yaml", "invalid:\n  - yaml\n  content: {missing", "Failed to load the secret file"),
            ("yml", "invalid:\n  - yaml\n  content: {missing", "Failed to load the secret file"),
            ("env", "INVALID_LINE_NO_EQUALS", "Invalid line format"),
        ],
    )
    def test_variables_import_invalid_format(self, tmp_path, format, invalid_content, error_pattern):
        """Test variables_import with invalid format files"""
        invalid_file = tmp_path / f"invalid.{format}"
        invalid_file.write_text(invalid_content)

        with pytest.raises(SystemExit, match=error_pattern):
            with create_session() as session:
                variable_command.variables_import(
                    self.parser.parse_args(["variables", "import", os.fspath(invalid_file)]),
                    session=session,
                )

    def test_variables_import_cross_format_compatibility(self, create_variable_file, simple_variable_data):
        """Test that the same variables can be imported from different formats consistently"""
        # Create files in both formats using the same test data
        json_file = create_variable_file(simple_variable_data, format="json")
        yaml_file = create_variable_file(simple_variable_data, format="yaml")

        # Test JSON import
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(json_file)]), session=session
            )

        json_results = {}
        for key in simple_variable_data:
            if key in ["key1"]:  # String values don't need JSON deserialization
                json_results[key] = Variable.get(key)
            else:
                json_results[key] = Variable.get(key, deserialize_json=True)

        # Clear variables
        for key in simple_variable_data:
            Variable.delete(key)

        # Test YAML import
        with create_session() as session:
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(yaml_file)]), session=session
            )

        yaml_results = {}
        for key in simple_variable_data:
            if key in ["key1"]:  # String values don't need JSON deserialization
                yaml_results[key] = Variable.get(key)
            else:
                yaml_results[key] = Variable.get(key, deserialize_json=True)

        # Compare results - both formats should produce identical results
        assert json_results == yaml_results
        assert json_results["key1"] == "value1"
        assert json_results["key2"] == {"nested": "dict", "with": ["list", "values"]}
        assert json_results["key3"] == 123
        assert json_results["key4"] is True
        assert json_results["key5"] is None
