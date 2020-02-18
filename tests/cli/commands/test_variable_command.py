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
import os
import unittest

from airflow import models
from airflow.bin import cli
from airflow.cli.commands import variable_command
from airflow.models import Variable


class TestCliVariables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def test_variables(self):
        # Checks if all subcommands are properly received
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"bar"}']))
        variable_command.variables_get(self.parser.parse_args([
            'variables', 'get', 'foo']))
        variable_command.variables_get(self.parser.parse_args([
            'variables', 'get', 'baz', '-d', 'bar']))
        variable_command.variables_list(self.parser.parse_args([
            'variables', 'list']))
        variable_command.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'bar']))
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', os.devnull]))
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', os.devnull]))

        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'original']))
        # First export
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables1.json']))

        first_exp = open('variables1.json', 'r')

        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'updated']))
        variable_command.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"oops"}']))
        variable_command.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'foo']))
        # First import
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables1.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))
        # Second export
        variable_command.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables2.json']))

        second_exp = open('variables2.json', 'r')
        self.assertEqual(first_exp.read(), second_exp.read())
        second_exp.close()
        first_exp.close()
        # Second import
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables2.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))

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
            'variables', 'export', 'variables3.json']))
        variable_command.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables3.json']))

        # Assert value
        self.assertEqual({'foo': 'oops'}, Variable.get('dict', deserialize_json=True))
        self.assertEqual(['oops'], Variable.get('list', deserialize_json=True))
        self.assertEqual('hello string', Variable.get('str'))  # cannot json.loads(str)
        self.assertEqual(42, Variable.get('int', deserialize_json=True))
        self.assertEqual(42.0, Variable.get('float', deserialize_json=True))
        self.assertEqual(True, Variable.get('true', deserialize_json=True))
        self.assertEqual(False, Variable.get('false', deserialize_json=True))
        self.assertEqual(None, Variable.get('null', deserialize_json=True))

        os.remove('variables1.json')
        os.remove('variables2.json')
        os.remove('variables3.json')
