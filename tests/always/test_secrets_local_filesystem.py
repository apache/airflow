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
import re
from contextlib import contextmanager
from unittest import mock

import pytest

from airflow.configuration import ensure_secrets_loaded
from airflow.exceptions import (
    AirflowException,
    AirflowFileParseException,
    ConnectionNotUnique,
)
from airflow.models import Variable
from airflow.secrets import local_filesystem
from airflow.secrets.local_filesystem import LocalFilesystemBackend

from tests_common.test_utils.config import conf_vars


@contextmanager
def mock_local_file(content):
    with mock.patch(
        "airflow.secrets.local_filesystem.open", mock.mock_open(read_data=content)
    ) as file_mock, mock.patch(
        "airflow.secrets.local_filesystem.os.path.exists", return_value=True
    ):
        yield file_mock


class TestFileParsers:
    @pytest.mark.parametrize(
        "content, expected_message",
        [
            (
                "AA",
                'Invalid line format. The line should contain at least one equal sign ("=")',
            ),
            ("=", "Invalid line format. Key is empty."),
        ],
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(
                AirflowFileParseException, match=re.escape(expected_message)
            ):
                local_filesystem.load_variables("a.env")

    @pytest.mark.parametrize(
        "content, expected_message",
        [
            ("[]", "The file should contain the object."),
            ("{AAAAA}", "Expecting property name enclosed in double quotes"),
            ("", "The file is empty."),
        ],
    )
    def test_json_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(
                AirflowFileParseException, match=re.escape(expected_message)
            ):
                local_filesystem.load_variables("a.json")


class TestLoadVariables:
    @pytest.mark.parametrize(
        "file_content, expected_variables",
        [
            ("", {}),
            ("KEY=AAA", {"KEY": "AAA"}),
            ("KEY_A=AAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ("KEY_A=AAA\n # AAAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            (
                "\n\n\n\nKEY_A=AAA\n\n\n\n\nKEY_B=BBB\n\n\n",
                {"KEY_A": "AAA", "KEY_B": "BBB"},
            ),
        ],
    )
    def test_env_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            variables = local_filesystem.load_variables("a.env")
            assert expected_variables == variables

    @pytest.mark.parametrize(
        "content, expected_message",
        [
            (
                "AA=A\nAA=B",
                "The \"a.env\" file contains multiple values for keys: ['AA']",
            ),
        ],
    )
    def test_env_file_invalid_logic(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(AirflowException, match=re.escape(expected_message)):
                local_filesystem.load_variables("a.env")

    @pytest.mark.parametrize(
        "file_content, expected_variables",
        [
            ({}, {}),
            ({"KEY": "AAA"}, {"KEY": "AAA"}),
            ({"KEY_A": "AAA", "KEY_B": "BBB"}, {"KEY_A": "AAA", "KEY_B": "BBB"}),
        ],
    )
    def test_json_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(json.dumps(file_content)):
            variables = local_filesystem.load_variables("a.json")
            assert expected_variables == variables

    @mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "File a.json was not found. Check the configuration of your Secrets backend."
            ),
        ):
            local_filesystem.load_variables("a.json")

    @pytest.mark.parametrize(
        "file_content, expected_variables",
        [
            ("KEY: AAA", {"KEY": "AAA"}),
            (
                """
            KEY_A: AAA
            KEY_B: BBB
            """,
                {"KEY_A": "AAA", "KEY_B": "BBB"},
            ),
        ],
    )
    def test_yaml_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            vars_yaml = local_filesystem.load_variables("a.yaml")
            vars_yml = local_filesystem.load_variables("a.yml")
            assert expected_variables == vars_yaml == vars_yml


class TestLoadConnection:
    @pytest.mark.parametrize(
        "file_content, expected_connection_uris",
        [
            ("CONN_ID=mysql://host_1/", {"CONN_ID": "mysql://host_1"}),
            (
                "CONN_ID1=mysql://host_1/\nCONN_ID2=mysql://host_2/",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
            (
                "CONN_ID1=mysql://host_1/\n # AAAA\nCONN_ID2=mysql://host_2/",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
            (
                "\n\n\n\nCONN_ID1=mysql://host_1/\n\n\n\n\nCONN_ID2=mysql://host_2/\n\n\n",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
        ],
    )
    def test_env_file_should_load_connection(
        self, file_content, expected_connection_uris
    ):
        with mock_local_file(file_content):
            connection_by_conn_id = local_filesystem.load_connections_dict("a.env")
            connection_uris_by_conn_id = {
                conn_id: connection.get_uri()
                for conn_id, connection in connection_by_conn_id.items()
            }

            assert expected_connection_uris == connection_uris_by_conn_id

    @pytest.mark.parametrize(
        "content, expected_connection_uris",
        [
            (
                "CONN_ID=mysql://host_1/?param1=val1&param2=val2",
                {"CONN_ID": "mysql://host_1/?param1=val1&param2=val2"},
            ),
        ],
    )
    def test_parsing_with_params(self, content, expected_connection_uris):
        with mock_local_file(content):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.env")
            connection_uris_by_conn_id = {
                conn_id: connection.get_uri()
                for conn_id, connection in connections_by_conn_id.items()
            }

            assert expected_connection_uris == connection_uris_by_conn_id

    @pytest.mark.parametrize(
        "content, expected_message",
        [
            (
                "AA",
                'Invalid line format. The line should contain at least one equal sign ("=")',
            ),
            ("=", "Invalid line format. Key is empty."),
        ],
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(
                AirflowFileParseException, match=re.escape(expected_message)
            ):
                local_filesystem.load_connections_dict("a.env")

    @pytest.mark.parametrize(
        "file_content, expected_connection_uris",
        [
            ({"CONN_ID": "mysql://host_1"}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": ["mysql://host_1"]}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": {"uri": "mysql://host_1"}}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": [{"uri": "mysql://host_1"}]}, {"CONN_ID": "mysql://host_1"}),
        ],
    )
    def test_json_file_should_load_connection(
        self, file_content, expected_connection_uris
    ):
        with mock_local_file(json.dumps(file_content)):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.json")
            connection_uris_by_conn_id = {
                conn_id: connection.get_uri()
                for conn_id, connection in connections_by_conn_id.items()
            }

            assert expected_connection_uris == connection_uris_by_conn_id

    @pytest.mark.parametrize(
        "file_content, expected_connection_uris",
        [
            ({"CONN_ID": None}, "Unexpected value type: <class 'NoneType'>."),
            ({"CONN_ID": 1}, "Unexpected value type: <class 'int'>."),
            ({"CONN_ID": [2]}, "Unexpected value type: <class 'int'>."),
            ({"CONN_ID": [None]}, "Unexpected value type: <class 'NoneType'>."),
            (
                {"CONN_ID": {"AAA": "mysql://host_1"}},
                "The object have illegal keys: AAA.",
            ),
            ({"CONN_ID": {"conn_id": "BBBB"}}, "Mismatch conn_id."),
            (
                {"CONN_ID": ["mysql://", "mysql://"]},
                "Found multiple values for CONN_ID in a.json.",
            ),
        ],
    )
    def test_env_file_invalid_input(self, file_content, expected_connection_uris):
        with mock_local_file(json.dumps(file_content)):
            with pytest.raises(
                AirflowException, match=re.escape(expected_connection_uris)
            ):
                local_filesystem.load_connections_dict("a.json")

    @mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "File a.json was not found. Check the configuration of your Secrets backend."
            ),
        ):
            local_filesystem.load_connections_dict("a.json")

    @pytest.mark.parametrize(
        "file_content, expected_attrs_dict",
        [
            (
                """CONN_A: 'mysql://host_a'""",
                {"CONN_A": {"conn_type": "mysql", "host": "host_a"}},
            ),
            (
                """
            conn_a: mysql://hosta
            conn_b:
               conn_type: scheme
               host: host
               schema: lschema
               login: Login
               password: None
               port: 1234
               extra_dejson:
                 arbitrary_dict:
                    a: b
                 keyfile_dict: '{"a": "b"}'
                 keyfile_path: asaa""",
                {
                    "conn_a": {"conn_type": "mysql", "host": "hosta"},
                    "conn_b": {
                        "conn_type": "scheme",
                        "host": "host",
                        "schema": "lschema",
                        "login": "Login",
                        "password": "None",
                        "port": 1234,
                        "extra_dejson": {
                            "arbitrary_dict": {"a": "b"},
                            "keyfile_dict": '{"a": "b"}',
                            "keyfile_path": "asaa",
                        },
                    },
                },
            ),
        ],
    )
    def test_yaml_file_should_load_connection(self, file_content, expected_attrs_dict):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.yaml")
            for conn_id, connection in connections_by_conn_id.items():
                expected_attrs = expected_attrs_dict[conn_id]
                actual_attrs = {k: getattr(connection, k) for k in expected_attrs.keys()}
                assert actual_attrs == expected_attrs

    @pytest.mark.parametrize(
        "file_content, expected_extras",
        [
            (
                """
                conn_c:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra_dejson:
                     aws_conn_id: bbb
                     region_name: ccc
                 """,
                {"conn_c": {"aws_conn_id": "bbb", "region_name": "ccc"}},
            ),
            (
                """
                conn_d:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra_dejson:
                     keyfile_dict:
                       a: b
                     key_path: xxx
                """,
                {
                    "conn_d": {
                        "keyfile_dict": {"a": "b"},
                        "key_path": "xxx",
                    }
                },
            ),
            (
                """
                conn_d:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra: '{\"keyfile_dict\": {\"a\": \"b\"}}'
                """,
                {"conn_d": {"keyfile_dict": {"a": "b"}}},
            ),
        ],
    )
    def test_yaml_file_should_load_connection_extras(self, file_content, expected_extras):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.yaml")
            connection_uris_by_conn_id = {
                conn_id: connection.extra_dejson
                for conn_id, connection in connections_by_conn_id.items()
            }
            assert expected_extras == connection_uris_by_conn_id

    @pytest.mark.parametrize(
        "file_content, expected_message",
        [
            (
                """conn_c:
               conn_type: scheme
               host: host
               schema: lschema
               login: Login
               password: None
               port: 1234
               extra:
                 abc: xyz
               extra_dejson:
                 aws_conn_id: bbb
                 region_name: ccc
                 """,
                "The extra and extra_dejson parameters are mutually exclusive.",
            ),
        ],
    )
    def test_yaml_invalid_extra(self, file_content, expected_message):
        with mock_local_file(file_content):
            with pytest.raises(AirflowException, match=re.escape(expected_message)):
                local_filesystem.load_connections_dict("a.yaml")

    @pytest.mark.parametrize(
        "file_content", ["CONN_ID=mysql://host_1/\nCONN_ID=mysql://host_2/"]
    )
    def test_ensure_unique_connection_env(self, file_content):
        with mock_local_file(file_content):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.env")

    @pytest.mark.parametrize(
        "file_content",
        [
            {"CONN_ID": ["mysql://host_1", "mysql://host_2"]},
            {"CONN_ID": [{"uri": "mysql://host_1"}, {"uri": "mysql://host_2"}]},
        ],
    )
    def test_ensure_unique_connection_json(self, file_content):
        with mock_local_file(json.dumps(file_content)):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.json")

    @pytest.mark.parametrize(
        "file_content",
        [
            """
            conn_a:
              - mysql://hosta
              - mysql://hostb"""
        ],
    )
    def test_ensure_unique_connection_yaml(self, file_content):
        with mock_local_file(file_content):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.yaml")

    @pytest.mark.parametrize("file_content", ["conn_a: mysql://hosta"])
    def test_yaml_extension_parsers_return_same_result(self, file_content):
        with mock_local_file(file_content):
            conn_uri_by_conn_id_yaml = {
                conn_id: conn.get_uri()
                for conn_id, conn in local_filesystem.load_connections_dict(
                    "a.yaml"
                ).items()
            }
            conn_uri_by_conn_id_yml = {
                conn_id: conn.get_uri()
                for conn_id, conn in local_filesystem.load_connections_dict(
                    "a.yml"
                ).items()
            }
            assert conn_uri_by_conn_id_yaml == conn_uri_by_conn_id_yml


class TestLocalFileBackend:
    def test_should_read_variable(self, tmp_path):
        path = tmp_path / "testfile.var.env"
        path.write_text("KEY_A=VAL_A")
        backend = LocalFilesystemBackend(variables_file_path=os.fspath(path))
        assert "VAL_A" == backend.get_variable("KEY_A")
        assert backend.get_variable("KEY_B") is None

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
            ("secrets", "backend_kwargs"): '{"variables_file_path": "var.env"}',
        }
    )
    def test_load_secret_backend_LocalFilesystemBackend(self):
        with mock_local_file("KEY_A=VAL_A"):
            backends = ensure_secrets_loaded()

            backend_classes = [backend.__class__.__name__ for backend in backends]
            assert "LocalFilesystemBackend" in backend_classes
            assert Variable.get("KEY_A") == "VAL_A"

    def test_should_read_connection(self, tmp_path):
        path = tmp_path / "testfile.env"
        path.write_text("CONN_A=mysql://host_a")
        backend = LocalFilesystemBackend(connections_file_path=os.fspath(path))
        assert "mysql://host_a" == backend.get_connection("CONN_A").get_uri()
        assert backend.get_variable("CONN_B") is None

    def test_files_are_optional(self):
        backend = LocalFilesystemBackend()
        assert None is backend.get_connection("CONN_A")
        assert backend.get_variable("VAR_A") is None
