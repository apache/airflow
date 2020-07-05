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

import json
import re
import unittest
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowFileParseException
from airflow.secrets import local_filesystem
from airflow.secrets.local_filesystem import LocalFilesystemBackend


@contextmanager
def mock_local_file(content):
    with mock.patch(
        "airflow.secrets.local_filesystem.open", mock.mock_open(read_data=content)
    ) as file_mock, mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=True):
        yield file_mock


class FileParsers(unittest.TestCase):
    @parameterized.expand(
        (
            ("AA", 'Invalid line format. The line should contain at least one equal sign ("=")'),
            ("=", "Invalid line format. Key is empty."),
        )
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with self.assertRaisesRegex(AirflowFileParseException, re.escape(expected_message)):
                local_filesystem.load_variables("a.env")

    @parameterized.expand(
        (
            ("[]", "The file should contain the object."),
            ("{AAAAA}", "Expecting property name enclosed in double quotes"),
            ("", "The file is empty."),
        )
    )
    def test_json_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with self.assertRaisesRegex(AirflowFileParseException, re.escape(expected_message)):
                local_filesystem.load_variables("a.json")


class TestLoadVariables(unittest.TestCase):
    @parameterized.expand(
        (
            ("", {}),
            ("KEY=AAA", {"KEY": "AAA"}),
            ("KEY_A=AAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ("KEY_A=AAA\n # AAAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ("\n\n\n\nKEY_A=AAA\n\n\n\n\nKEY_B=BBB\n\n\n", {"KEY_A": "AAA", "KEY_B": "BBB"}),
        )
    )
    def test_env_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            variables = local_filesystem.load_variables("a.env")
            self.assertEqual(expected_variables, variables)

    @parameterized.expand((("AA=A\nAA=B", "The \"a.env\" file contains multiple values for keys: ['AA']"),))
    def test_env_file_invalid_logic(self, content, expected_message):
        with mock_local_file(content):
            with self.assertRaisesRegex(AirflowException, re.escape(expected_message)):
                local_filesystem.load_variables("a.env")

    @parameterized.expand(
        (
            ({}, {}),
            ({"KEY": "AAA"}, {"KEY": "AAA"}),
            ({"KEY_A": "AAA", "KEY_B": "BBB"}, {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ({"KEY_A": "AAA", "KEY_B": "BBB"}, {"KEY_A": "AAA", "KEY_B": "BBB"}),
        )
    )
    def test_json_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(json.dumps(file_content)):
            variables = local_filesystem.load_variables("a.json")
            self.assertEqual(expected_variables, variables)

    @mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with self.assertRaisesRegex(
            AirflowException,
            re.escape("File a.json was not found. Check the configuration of your Secrets backend."),
        ):
            local_filesystem.load_variables("a.json")

    @parameterized.expand(
        (
            ("KEY: AAA", {"KEY": "AAA"}),
            ("""
            KEY_A: AAA
            KEY_B: BBB
            """, {"KEY_A": "AAA", "KEY_B": "BBB"}),
        )
    )
    def test_yaml_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            variables = local_filesystem.load_variables('a.yaml')
            self.assertEqual(expected_variables, variables)


class TestLoadConnection(unittest.TestCase):
    @parameterized.expand(
        (
            ("CONN_ID=mysql://host_1/", {"CONN_ID": ["mysql://host_1"]}),
            (
                "CONN_ID=mysql://host_1/\nCONN_ID=mysql://host_2/",
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            ),
            (
                "CONN_ID=mysql://host_1/\n # AAAA\nCONN_ID=mysql://host_2/",
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            ),
            (
                "\n\n\n\nCONN_ID=mysql://host_1/\n\n\n\n\nCONN_ID=mysql://host_2/\n\n\n",
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            ),
        )
    )
    def test_env_file_should_load_connection(self, file_content, expected_connection_uris):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections("a.env")
            connection_uris_by_conn_id = {
                conn_id: [connection.get_uri() for connection in connections]
                for conn_id, connections in connections_by_conn_id.items()
            }

            self.assertEqual(expected_connection_uris, connection_uris_by_conn_id)

    @parameterized.expand(
        (
            ("AA", 'Invalid line format. The line should contain at least one equal sign ("=")'),
            ("=", "Invalid line format. Key is empty."),
        )
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with self.assertRaisesRegex(AirflowFileParseException, re.escape(expected_message)):
                local_filesystem.load_connections("a.env")

    @parameterized.expand(
        (
            ({"CONN_ID": "mysql://host_1"}, {"CONN_ID": ["mysql://host_1"]}),
            ({"CONN_ID": ["mysql://host_1"]}, {"CONN_ID": ["mysql://host_1"]}),
            (
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            ),
            ({"CONN_ID": {"uri": "mysql://host_1"}}, {"CONN_ID": ["mysql://host_1"]}),
            ({"CONN_ID": [{"uri": "mysql://host_1"}]}, {"CONN_ID": ["mysql://host_1"]}),
            (
                {"CONN_ID": [{"uri": "mysql://host_1"}, {"uri": "mysql://host_2"}]},
                {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            ),
        )
    )
    def test_json_file_should_load_connection(self, file_content, expected_connection_uris):
        with mock_local_file(json.dumps(file_content)):
            connections_by_conn_id = local_filesystem.load_connections("a.json")
            connection_uris_by_conn_id = {
                conn_id: [connection.get_uri() for connection in connections]
                for conn_id, connections in connections_by_conn_id.items()
            }

            self.assertEqual(expected_connection_uris, connection_uris_by_conn_id)

    @parameterized.expand(
        (
            ({"CONN_ID": None}, "Unexpected value type: <class 'NoneType'>."),
            ({"CONN_ID": 1}, "Unexpected value type: <class 'int'>."),
            ({"CONN_ID": [2]}, "Unexpected value type: <class 'int'>."),
            ({"CONN_ID": ["mysql://host_1", None]}, "Unexpected value type: <class 'NoneType'>."),
            ({"CONN_ID": {"AAA": "mysql://host_1"}}, "The object have illegal keys: AAA."),
            ({"CONN_ID": {"conn_id": "BBBB"}}, "Mismatch conn_id."),
        )
    )
    def test_env_file_invalid_input(self, file_content, expected_connection_uris):
        with mock_local_file(json.dumps(file_content)):
            with self.assertRaisesRegex(AirflowException, re.escape(expected_connection_uris)):
                local_filesystem.load_connections("a.json")

    @mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with self.assertRaisesRegex(
            AirflowException,
            re.escape("File a.json was not found. Check the configuration of your Secrets backend."),
        ):
            local_filesystem.load_connections("a.json")

    @parameterized.expand(
        (
            ("""CONN_A: 'mysql://host_a'""", {"CONN_A": ["mysql://host_a"]}),
            ("""
            CONN_B:
                - 'mysql://host_a'
                - 'mysql://host_b'
             """, {"CONN_B": ["mysql://host_a", "mysql://host_b"]}),
            ("""
            conn_a: mysql://hosta
            conn_b:
              - mysql://hostb
              - mysql://hostc
            conn_c:
               conn_type: scheme
               host: host
               schema: lschema
               login: Login
               password: None
               port: 1234""",
                {"conn_a": ["mysql://hosta"], "conn_b": ["mysql://hostb", "mysql://hostc"],
                    "conn_c": ["scheme://Login:None@host:1234/lschema"]}),
        )
    )
    def test_yaml_file_should_load_connection(self, file_content, expected_connection_uris):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections("a.yaml")
            connection_uris_by_conn_id = {
                conn_id: [connection.get_uri() for connection in connections]
                for conn_id, connections in connections_by_conn_id.items()
            }

            self.assertEqual(expected_connection_uris, connection_uris_by_conn_id)


class TestLocalFileBackend(unittest.TestCase):
    def test_should_read_variable(self):
        with NamedTemporaryFile(suffix="var.env") as tmp_file:
            tmp_file.write("KEY_A=VAL_A".encode())
            tmp_file.flush()
            backend = LocalFilesystemBackend(variables_file_path=tmp_file.name)
            self.assertEqual("VAL_A", backend.get_variable("KEY_A"))
            self.assertIsNone(backend.get_variable("KEY_B"))

    def test_should_read_connection(self):
        with NamedTemporaryFile(suffix=".env") as tmp_file:
            tmp_file.write("CONN_A=mysql://host_a\nCONN_A=mysql://host_b".encode())
            tmp_file.flush()
            backend = LocalFilesystemBackend(connections_file_path=tmp_file.name)
            self.assertEqual(
                ["mysql://host_a", "mysql://host_b"],
                [conn.get_uri() for conn in backend.get_connections("CONN_A")],
            )
            self.assertIsNone(backend.get_variable("CONN_B"))

    def test_files_are_optional(self):
        backend = LocalFilesystemBackend()
        self.assertEqual([], backend.get_connections("CONN_A"))
        self.assertIsNone(backend.get_variable("VAR_A"))
