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
import yaml
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkActionResponse,
    BulkResponse,
)
from sqlalchemy import select

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import variable_command
from airflow.models import Variable
from airflow.secrets.local_filesystem import load_variables
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test


def _apply_import_via_model(file_path, action_on_existing: str = "overwrite") -> None:
    """Apply a variables file via the model, mirroring the pre-migration import.

    ``variables import`` is migrated to the airflowctl client, so round-trip tests that
    need to actually persist an imported file use this helper instead of the API-backed command.
    """
    var_json = load_variables(file_path)
    existing_keys: set[str] = set()
    if action_on_existing != "overwrite":
        with create_session() as session:
            existing_keys = set(session.scalars(select(Variable.key).where(Variable.key.in_(var_json))))
    if action_on_existing == "fail" and existing_keys:
        raise SystemExit(f"Failed. These keys: {sorted(existing_keys)} already exists.")
    for k, v in var_json.items():
        if action_on_existing == "skip" and k in existing_keys:
            continue
        value, description = v, None
        if isinstance(v, dict) and "value" in v:
            value, description = v["value"], v.get("description")
        Variable.set(k, value, description, serialize_json=not isinstance(value, str))


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
        cls.dagbag = models.DagBag()
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

    def test_variables_export_preserves_types(self, tmp_path):
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

        export_file = tmp_path / "variables_types.json"
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", os.fspath(export_file)])
        )

        assert json.loads(export_file.read_text()) == {
            "dict": {"foo": "oops"},
            "list": ["oops"],
            "str": "hello string",
            "int": 42,
            "float": 42.0,
            "true": True,
            "false": False,
            "null": None,
        }

    def test_variables_list(self):
        """Test variable_list command"""
        # Test command is received
        variable_command.variables_list(self.parser.parse_args(["variables", "list"]))

    def test_variables_list_show_values(self):
        """Test variables list with --show-values flag shows actual values."""
        # Create test variables
        Variable.set("test_key1", "test_value1")
        Variable.set("test_key2", "test_value2")

        args = self.parser.parse_args(["variables", "list", "--output", "json", "--show-values"])
        with redirect_stdout(StringIO()) as stdout_io:
            variable_command.variables_list(args)
            output = stdout_io.getvalue()

        # Parse JSON output and verify values are shown
        data = json.loads(output)
        assert len(data) >= 2
        key_value_map = {item["key"]: item["val"] for item in data}
        assert "test_value1" in key_value_map["test_key1"]
        assert "test_value2" in key_value_map["test_key2"]

    def test_variables_list_hide_sensitive(self):
        """Test variables list with --hide-sensitive masks all values."""
        # Create test variables
        Variable.set("test_key1", "test_value1")
        Variable.set("test_key2", "test_value2")

        args = self.parser.parse_args(
            ["variables", "list", "--output", "json", "--show-values", "--hide-sensitive"]
        )
        with redirect_stdout(StringIO()) as stdout_io:
            variable_command.variables_list(args)
            output = stdout_io.getvalue()

        # Parse JSON output and verify values are masked
        data = json.loads(output)
        assert len(data) >= 2
        for item in data:
            if "test_key" in item["key"]:
                assert item["val"] == "***"

    def test_variables_list_hide_sensitive_without_show_values_fails(self):
        """--hide-sensitive without --show-values should fail."""
        args = self.parser.parse_args(["variables", "list", "--hide-sensitive"])
        with pytest.raises(SystemExit, match="--hide-sensitive can only be used with --show-values"):
            variable_command.variables_list(args)

    def test_variables_list_default_hides_values(self):
        """By default, variables list should only show keys, not values."""
        Variable.set("test_key1", "test_value1")
        Variable.set("test_key2", "test_value2")

        args = self.parser.parse_args(["variables", "list", "--output", "json"])
        with redirect_stdout(StringIO()) as stdout_io:
            variable_command.variables_list(args)
            output = stdout_io.getvalue()

        data = json.loads(output)
        assert len(data) >= 2
        for item in data:
            if "test_key" in item["key"]:
                assert "val" not in item

    def test_variables_list_edge_cases(self):
        """Test variables list with None and empty values."""
        Variable.set("empty_var", "")
        Variable.set("none_var", None)
        Variable.set("normal_var", "normal_value")

        args = self.parser.parse_args(["variables", "list", "--output", "json", "--show-values"])
        with redirect_stdout(StringIO()) as stdout_io:
            variable_command.variables_list(args)
            output = stdout_io.getvalue()

        data = json.loads(output)
        key_value_map = {item["key"]: item["val"] for item in data}

        assert key_value_map["empty_var"] == ""
        assert key_value_map["none_var"] == "None"
        assert key_value_map["normal_var"] == "normal_value"

        args = self.parser.parse_args(
            ["variables", "list", "--output", "json", "--show-values", "--hide-sensitive"]
        )
        with redirect_stdout(StringIO()) as stdout_io:
            variable_command.variables_list(args)
            output = stdout_io.getvalue()

        data = json.loads(output)
        for item in data:
            if item["key"] in ["empty_var", "none_var", "normal_var"]:
                assert item["val"] == "***"

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
    def test_variables_import_unsupported_format(self, mock_cli_api_client, tmp_path, filename):
        """Test variables_import command with unsupported file formats"""
        # Use devnull directly or create a file with unsupported extension
        if filename == os.devnull:
            file_path = filename
        else:
            file_path = tmp_path / filename
            file_path.write_text("some content")
            file_path = os.fspath(file_path)

        with pytest.raises(SystemExit, match=r"Unsupported file format"):
            variable_command.variables_import(self.parser.parse_args(["variables", "import", file_path]))
        mock_cli_api_client.variables.bulk.assert_not_called()

    def test_variables_export(self):
        """Test variables_export command"""
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.devnull]))

    def test_variables_export_round_trip(self, tmp_path):
        """Test isolation of variables"""
        path1 = tmp_path / "testfile1.json"
        path2 = tmp_path / "testfile2.json"

        # First export
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", '{"foo":"bar"}']))
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "bar", "original"]))
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.fspath(path1)]))

        # Mutate the state, then restore it from the first export and export again
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "bar", "updated"]))
        variable_command.variables_set(self.parser.parse_args(["variables", "set", "foo", '{"foo":"oops"}']))
        variable_command.variables_delete(self.parser.parse_args(["variables", "delete", "foo"]))
        _apply_import_via_model(os.fspath(path1))

        assert Variable.get("bar") == "original"
        assert Variable.get("foo") == '{\n  "foo": "bar"\n}'

        # Second export
        variable_command.variables_export(self.parser.parse_args(["variables", "export", os.fspath(path2)]))

        assert path1.read_text() == path2.read_text()

    def test_variables_export_with_description(self, tmp_path):
        """Test variables_export with file-description parameter"""
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

    @pytest.mark.parametrize("format", ["json", "yaml", "yml"])
    def test_variables_import_formats(
        self, mock_cli_api_client, create_variable_file, simple_variable_data, format
    ):
        """Test variables_import with different formats (JSON, YAML, YML)"""
        file = create_variable_file(simple_variable_data, format=format)
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=list(simple_variable_data))
        )

        variable_command.variables_import(self.parser.parse_args(["variables", "import", os.fspath(file)]))

        action = mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0]
        entities = {e.key: e.value.root for e in action.entities}
        assert entities["key1"] == "value1"
        assert entities["key2"] == {"nested": "dict", "with": ["list", "values"]}
        assert entities["key3"] == 123
        assert entities["key4"] is True
        assert entities["key5"] is None
        assert action.action_on_existence == BulkActionOnExistence.OVERWRITE

    @pytest.mark.parametrize("format", ["json", "yaml"])
    def test_variables_import_with_descriptions(
        self, mock_cli_api_client, create_variable_file, variable_data_with_descriptions, format
    ):
        """Test variables_import with descriptions in different formats (JSON, YAML)"""
        file = create_variable_file(variable_data_with_descriptions, format=format)
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=list(variable_data_with_descriptions))
        )

        variable_command.variables_import(self.parser.parse_args(["variables", "import", os.fspath(file)]))

        entities = {
            e.key: e
            for e in mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].entities
        }
        assert entities["var1"].value.root == "test_value"
        assert entities["var1"].description == "Test description for var1"
        assert entities["var2"].value.root == {"complex": "object"}
        assert entities["var2"].description == "Complex variable"
        assert entities["var3"].value.root == "simple_value"
        assert entities["var3"].description is None

    @pytest.mark.parametrize(
        "value",
        ["", 0, False, None],
        ids=["empty_string", "zero", "false", "null"],
    )
    def test_variables_import_with_structured_falsy_values(
        self, mock_cli_api_client, create_variable_file, value
    ):
        """Test variables_import preserves structured falsy values and descriptions."""
        file = create_variable_file(
            {"falsy_key": {"value": value, "description": "Falsy value description"}},
            format="json",
        )
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=["falsy_key"])
        )

        variable_command.variables_import(self.parser.parse_args(["variables", "import", os.fspath(file)]))

        entity = mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].entities[0]
        assert entity.key == "falsy_key"
        assert entity.value.root == value
        assert entity.description == "Falsy value description"

    def test_variables_import_env(self, mock_cli_api_client, create_variable_file, env_variable_data):
        """Test variables_import with ENV format"""
        env_file = create_variable_file(env_variable_data, format="env")
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=list(env_variable_data))
        )

        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(env_file)])
        )

        entities = {
            e.key: e.value.root
            for e in mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].entities
        }
        assert entities["KEY_A"] == "value_a"
        assert entities["KEY_B"] == "value with spaces"
        assert entities["KEY_C"] == '{"json": "value", "number": 42}'
        assert entities["KEY_D"] == "true"
        assert entities["KEY_E"] == "123"

    @pytest.mark.parametrize("format", ["json", "yaml", "yml"])
    def test_variables_import_action_on_existing(
        self, mock_cli_api_client, create_variable_file, simple_variable_data, format
    ):
        """Test variables_import with action_on_existing_key parameter for different formats"""
        file = create_variable_file(simple_variable_data, format=format)
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=list(simple_variable_data))
        )

        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", os.fspath(file), "--action-on-existing-key", "skip"]
            )
        )
        assert (
            mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].action_on_existence
            == BulkActionOnExistence.SKIP
        )

        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", os.fspath(file), "--action-on-existing-key", "overwrite"]
            )
        )
        assert (
            mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].action_on_existence
            == BulkActionOnExistence.OVERWRITE
        )

        # fail: the server reports the conflict via the bulk response errors, so the command exits
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(
                errors=[
                    {"error": "The variables with these keys: {'key1'} already exist.", "status_code": 409}
                ]
            )
        )
        with pytest.raises(SystemExit, match="Failed to import variables"):
            variable_command.variables_import(
                self.parser.parse_args(
                    ["variables", "import", os.fspath(file), "--action-on-existing-key", "fail"]
                )
            )

    def test_variables_import_env_action_on_existing(self, mock_cli_api_client, tmp_path):
        """Test variables_import ENV with action_on_existing_key parameter"""
        env_file = tmp_path / "variables_update.env"
        env_content = """EXISTING_VAR=updated_value
NEW_VAR=fresh_value"""
        env_file.write_text(env_content)
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=["EXISTING_VAR", "NEW_VAR"])
        )

        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", os.fspath(env_file), "--action-on-existing-key", "skip"]
            )
        )
        action = mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0]
        assert action.action_on_existence == BulkActionOnExistence.SKIP
        assert {e.key: e.value.root for e in action.entities} == {
            "EXISTING_VAR": "updated_value",
            "NEW_VAR": "fresh_value",
        }

        variable_command.variables_import(
            self.parser.parse_args(
                ["variables", "import", os.fspath(env_file), "--action-on-existing-key", "overwrite"]
            )
        )
        assert (
            mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].action_on_existence
            == BulkActionOnExistence.OVERWRITE
        )

    @pytest.mark.parametrize(
        ("format", "invalid_content", "error_pattern"),
        [
            ("json", '{"invalid": "json", missing_quotes: true}', "Failed to load the secret file"),
            ("yaml", "invalid:\n  - yaml\n  content: {missing", "Failed to load the secret file"),
            ("yml", "invalid:\n  - yaml\n  content: {missing", "Failed to load the secret file"),
            ("env", "INVALID_LINE_NO_EQUALS", "Invalid line format"),
        ],
    )
    def test_variables_import_invalid_format(
        self, mock_cli_api_client, tmp_path, format, invalid_content, error_pattern
    ):
        """Test variables_import with invalid format files"""
        invalid_file = tmp_path / f"invalid.{format}"
        invalid_file.write_text(invalid_content)

        with pytest.raises(SystemExit, match=error_pattern):
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.fspath(invalid_file)])
            )
        mock_cli_api_client.variables.bulk.assert_not_called()

    def test_variables_import_cross_format_compatibility(
        self, mock_cli_api_client, create_variable_file, simple_variable_data
    ):
        """Test that the same variables can be imported from different formats consistently"""
        # Create files in both formats using the same test data
        json_file = create_variable_file(simple_variable_data, format="json")
        yaml_file = create_variable_file(simple_variable_data, format="yaml")
        mock_cli_api_client.variables.bulk.return_value = BulkResponse(
            create=BulkActionResponse(success=list(simple_variable_data))
        )

        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(json_file)])
        )
        json_results = {
            e.key: e.value.root
            for e in mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].entities
        }

        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(yaml_file)])
        )
        yaml_results = {
            e.key: e.value.root
            for e in mock_cli_api_client.variables.bulk.call_args.kwargs["variables"].actions[0].entities
        }

        assert json_results == yaml_results
        assert json_results["key1"] == "value1"
        assert json_results["key2"] == {"nested": "dict", "with": ["list", "values"]}
        assert json_results["key3"] == 123
        assert json_results["key4"] is True
        assert json_results["key5"] is None
