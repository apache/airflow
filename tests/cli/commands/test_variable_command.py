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
#
import io
import os
import tempfile
import unittest.mock
from contextlib import redirect_stdout

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import variable_command
from airflow.models import Variable
from tests.test_utils.db import clear_db_variables


class TestCliVariables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    def setUp(self):
        clear_db_variables()

    def tearDown(self):
        clear_db_variables()

    def test_variables_set(self):
        """Test variable_set command"""
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', 'bar']))
        self.assertIsNotNone(Variable.get("foo"))
        self.assertRaises(KeyError, Variable.get, "foo1")

    def test_variables_get(self):
        Variable.set('foo', {'foo': 'bar'}, serialize_json=True)

        with redirect_stdout(io.StringIO()) as stdout:
            variable_command.variables_get(self.parser.parse_args([
                'variables', 'get', 'foo']))
            self.assertEqual('{\n  "foo": "bar"\n}\n', stdout.getvalue())

    def test_get_variable_default_value(self):
        with redirect_stdout(io.StringIO()) as stdout:
            variable_command.variables_get(self.parser.parse_args([
                'variables', 'get', 'baz', '--default', 'bar']))
            self.assertEqual("bar\n", stdout.getvalue())

    def test_get_variable_missing_variable(self):
        with self.assertRaises(SystemExit):
            variable_command.variables_get(self.parser.parse_args([
                'variables', 'get', 'no-existing-VAR']))

    def test_variables_set_different_types(self):
        """Test storage of various data types"""
        # Set a dict
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'dict', '{"foo": "oops"}']))
        # Set a list
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'list', '["oops"]']))
        # Set str
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'str', 'hello string']))
        # Set int
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'int', '42']))
        # Set float
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'float', '42.0']))
        # Set true
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'true', 'true']))
        # Set false
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'false', 'false']))
        # Set none
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'null', 'null']))

        # Export and then import
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables_types.json']))
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables_types.json']))

        # Assert value
        self.assertEqual({'foo': 'oops'}, Variable.get('dict', deserialize_json=True))
        self.assertEqual(['oops'], Variable.get('list', deserialize_json=True))
        self.assertEqual('hello string', Variable.get('str'))  # cannot json.loads(str)
        self.assertEqual(42, Variable.get('int', deserialize_json=True))
        self.assertEqual(42.0, Variable.get('float', deserialize_json=True))
        self.assertEqual(True, Variable.get('true', deserialize_json=True))
        self.assertEqual(False, Variable.get('false', deserialize_json=True))
        self.assertEqual(None, Variable.get('null', deserialize_json=True))

        os.remove('variables_types.json')

    def test_variables_list(self):
        """Test variable_list command"""
        # Test command is received
        variable_command.variables_list(self.parser.parse_args([
            'variables', 'list']))

    def test_variables_delete(self):
        """Test variable_delete command"""
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', 'bar']))
        variable_command.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'foo']))
        self.assertRaises(KeyError, Variable.get, "foo")

    def test_variables_import(self):
        """Test variables_import command"""
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', os.devnull]))

    def test_variables_export(self):
        """Test variables_export command"""
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', os.devnull]))

    def test_variables_isolation(self):
        """Test isolation of variables"""
        tmp1 = tempfile.NamedTemporaryFile(delete=True)
        tmp2 = tempfile.NamedTemporaryFile(delete=True)

        # First export
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"bar"}']))
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'original']))
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', tmp1.name]))

        first_exp = open(tmp1.name, 'r')

        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'updated']))
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"oops"}']))
        variable_command.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'foo']))
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', tmp1.name]))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))

        # Second export
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', tmp2.name]))

        second_exp = open(tmp2.name, 'r')
        self.assertEqual(first_exp.read(), second_exp.read())

        # Clean up files
        second_exp.close()
        first_exp.close()
