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

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


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
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo", "bar"])
        )
        assert Variable.get("foo") is not None
        with pytest.raises(KeyError):
            Variable.get("foo1")

    def test_variables_set_with_description(self):
        """Test variable_set command with optional description argument"""
        expected_var_desc = "foo_bar_description"
        var_key = "foo"
        variable_command.variables_set(
            self.parser.parse_args(
                ["variables", "set", var_key, "bar", "--description", expected_var_desc]
            )
        )

        assert Variable.get(var_key) == "bar"
        with create_session() as session:
            actual_var_desc = session.scalar(
                select(Variable.description).where(Variable.key == var_key)
            )
            assert actual_var_desc == expected_var_desc

        with pytest.raises(KeyError):
            Variable.get("foo1")

    def test_variables_get(self):
        Variable.set("foo", {"foo": "bar"}, serialize_json=True)

        with redirect_stdout(StringIO()) as stdout:
            variable_command.variables_get(
                self.parser.parse_args(["variables", "get", "foo"])
            )
            assert '{\n  "foo": "bar"\n}\n' == stdout.getvalue()

    def test_get_variable_default_value(self):
        with redirect_stdout(StringIO()) as stdout:
            variable_command.variables_get(
                self.parser.parse_args(["variables", "get", "baz", "--default", "bar"])
            )
            assert "bar\n" == stdout.getvalue()

    def test_get_variable_missing_variable(self):
        with pytest.raises(SystemExit):
            variable_command.variables_get(
                self.parser.parse_args(["variables", "get", "no-existing-VAR"])
            )

    def test_variables_set_different_types(self):
        """Test storage of various data types"""
        # Set a dict
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "dict", '{"foo": "oops"}'])
        )
        # Set a list
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "list", '["oops"]'])
        )
        # Set str
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "str", "hello string"])
        )
        # Set int
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "int", "42"])
        )
        # Set float
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "float", "42.0"])
        )
        # Set true
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "true", "true"])
        )
        # Set false
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "false", "false"])
        )
        # Set none
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "null", "null"])
        )

        # Export and then import
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", "variables_types.json"])
        )
        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", "variables_types.json"])
        )

        # Assert value
        assert {"foo": "oops"} == Variable.get("dict", deserialize_json=True)
        assert ["oops"] == Variable.get("list", deserialize_json=True)
        assert "hello string" == Variable.get("str")  # cannot json.loads(str)
        assert 42 == Variable.get("int", deserialize_json=True)
        assert 42.0 == Variable.get("float", deserialize_json=True)
        assert Variable.get("true", deserialize_json=True) is True
        assert Variable.get("false", deserialize_json=True) is False
        assert Variable.get("null", deserialize_json=True) is None

        # test variable import skip existing
        # set varliable list to ["airflow"] and have it skip during import
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "list", '["airflow"]'])
        )
        variable_command.variables_import(
            self.parser.parse_args(
                [
                    "variables",
                    "import",
                    "variables_types.json",
                    "--action-on-existing-key",
                    "skip",
                ]
            )
        )
        assert ["airflow"] == Variable.get(
            "list", deserialize_json=True
        )  # should not be overwritten

        # test variable import fails on existing when action is set to fail
        with pytest.raises(SystemExit):
            variable_command.variables_import(
                self.parser.parse_args(
                    [
                        "variables",
                        "import",
                        "variables_types.json",
                        "--action-on-existing-key",
                        "fail",
                    ]
                )
            )

        os.remove("variables_types.json")

    def test_variables_list(self):
        """Test variable_list command"""
        # Test command is received
        variable_command.variables_list(self.parser.parse_args(["variables", "list"]))

    def test_variables_delete(self):
        """Test variable_delete command"""
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo", "bar"])
        )
        variable_command.variables_delete(
            self.parser.parse_args(["variables", "delete", "foo"])
        )
        with pytest.raises(KeyError):
            Variable.get("foo")

    def test_variables_import(self):
        """Test variables_import command"""
        with pytest.raises(SystemExit, match=r"Invalid variables file"):
            variable_command.variables_import(
                self.parser.parse_args(["variables", "import", os.devnull])
            )

    def test_variables_export(self):
        """Test variables_export command"""
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", os.devnull])
        )

    def test_variables_isolation(self, tmp_path):
        """Test isolation of variables"""
        path1 = tmp_path / "testfile1"
        path2 = tmp_path / "testfile2"

        # First export
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo", '{"foo":"bar"}'])
        )
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "bar", "original"])
        )
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", os.fspath(path1)])
        )

        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "bar", "updated"])
        )
        variable_command.variables_set(
            self.parser.parse_args(["variables", "set", "foo", '{"foo":"oops"}'])
        )
        variable_command.variables_delete(
            self.parser.parse_args(["variables", "delete", "foo"])
        )
        variable_command.variables_import(
            self.parser.parse_args(["variables", "import", os.fspath(path1)])
        )

        assert "original" == Variable.get("bar")
        assert '{\n  "foo": "bar"\n}' == Variable.get("foo")

        # Second export
        variable_command.variables_export(
            self.parser.parse_args(["variables", "export", os.fspath(path2)])
        )

        assert path1.read_text() == path2.read_text()
