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
import shlex
import warnings
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pytest
import yaml

from airflow.cli import cli_parser
from airflow.cli.api.datamodels._generated import (
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
)
from airflow.cli.commands.remote_commands import connection_command
from airflow.exceptions import AirflowException

from tests_common.test_utils.db import clear_db_connections

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module", autouse=True)
def clear_connections():
    yield
    clear_db_connections(add_default_connections_back=False)


class TestCliConnection:
    parser = cli_parser.get_parser()
    connection = ConnectionBody(
        connection_id="google_cloud_default",
        conn_type="google_cloud_platform",
    )

    _expected_cons = [
        {"connection_id": "airflow_db", "conn_type": "mysql"},
        {"connection_id": "google_cloud_default", "conn_type": "google_cloud_platform"},
        {"connection_id": "http_default", "conn_type": "http"},
        {"connection_id": "local_mysql", "conn_type": "mysql"},
        {"connection_id": "mongo_default", "conn_type": "mongo"},
        {"connection_id": "mssql_default", "conn_type": "mssql"},
        {"connection_id": "mysql_default", "conn_type": "mysql"},
        {"connection_id": "pinot_broker_default", "conn_type": "pinot"},
        {"connection_id": "postgres_default", "conn_type": "postgres"},
        {"connection_id": "presto_default", "conn_type": "presto"},
        {"connection_id": "sqlite_default", "conn_type": "sqlite"},
        {"connection_id": "trino_default", "conn_type": "trino"},
        {"connection_id": "vertica_default", "conn_type": "vertica"},
    ]
    connections = ConnectionCollectionResponse(
        connections=[
            ConnectionResponse(connection_id=connection["connection_id"], conn_type=connection["conn_type"])
            for connection in _expected_cons
        ],
        total_entries=len(_expected_cons),
    )
    expected_connections: dict = {}
    expected_connections_as_yaml = ()
    expected_connections_as_env: list = []
    empty_connections = ConnectionCollectionResponse(connections=[], total_entries=0)

    connection_test_response = ConnectionTestResponse(status=True, message="Connection success!")
    connection_failed_test_response = ConnectionTestResponse(status=False, message="Connection failed!")

    def setup_method(self):
        clear_db_connections(add_default_connections_back=True)
        if not self.expected_connections:
            for connection in self.connections.connections:
                self.expected_connections.update(
                    {
                        connection.connection_id: {
                            "conn_type": connection.conn_type,
                            "description": connection.description,
                            "host": connection.host,
                            "login": connection.login,
                            "password": connection.password,
                            "schema": connection.schema_,
                            "port": connection.port,
                            "extra": connection.extra,
                        }
                    }
                )
        if not self.expected_connections_as_env:
            for key, value in self.expected_connections.items():
                self.expected_connections_as_env.append(f"{key}={value['conn_type'].replace('_', '-')}://")

        if not self.expected_connections_as_yaml:
            self.expected_connections_as_yaml = yaml.dump(self.expected_connections)


class TestCliGetConnection(TestCliConnection):
    def test_cli_connection_get(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections/google_cloud_default",
            response_json=self.connection.model_dump(),
            expected_http_status_code=200,
        )

        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_get(
                self.parser.parse_args(["connections", "get", "google_cloud_default", "--output", "json"]),
                cli_api_client=cli_api_client,
            )
        stdout = stdout.getvalue()
        assert self.connection.connection_id in stdout
        assert self.connection.conn_type in stdout

    def test_cli_connection_get_invalid(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections/INVALID",
            response_json={"detail": "Connection not found."},
            expected_http_status_code=404,
        )
        with pytest.raises(SystemExit), redirect_stdout(StringIO()) as stdout:
            connection_command.connections_get(
                self.parser.parse_args(["connections", "get", "INVALID"]),
                cli_api_client=cli_api_client,
            )
        stdout = stdout.getvalue()
        assert "Connection not found." in stdout


class TestCliListConnections(TestCliConnection):
    def test_cli_connections_list_as_json(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )

        args = self.parser.parse_args(["connections", "list", "--output", "json"])
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_list(args, cli_api_client=cli_api_client)
            print(stdout.getvalue())
            stdout = stdout.getvalue()

        for connection_id, connection in self.expected_connections.items():
            assert connection_id in stdout
            assert connection["conn_type"] in stdout

    def test_cli_connections_filter_conn_id(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        args = self.parser.parse_args(
            ["connections", "list", "--output", "json", "--conn-id", "http_default"]
        )
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_list(args, cli_api_client=cli_api_client)
            stdout = stdout.getvalue()
        assert "http_default" in stdout


class TestCliExportConnections(TestCliConnection):
    def test_cli_connections_export_should_return_error_for_invalid_command(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["connections", "export"])

    def test_cli_connections_export_should_return_error_for_invalid_format(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["connections", "export", "--format", "invalid", "/path/to/file"])

    def test_cli_connections_export_should_return_error_for_invalid_export_format(self, tmp_path):
        output_filepath = tmp_path / "connections.invalid"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        with pytest.raises(SystemExit, match=r"Unsupported file format"):
            connection_command.connections_export(args)

    def test_cli_connections_export_should_not_raise_error_if_connections_is_empty(
        self, tmp_path, cli_api_client_maker
    ):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.empty_connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path / "connections.json"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args, cli_api_client=cli_api_client)
        assert output_filepath.read_text() == "{}"

    def test_cli_connections_export_should_export_as_json(self, tmp_path, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path / "connections.json"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args, cli_api_client=cli_api_client)
        expected_connections = self.expected_connections
        assert json.loads(output_filepath.read_text()) == expected_connections

    def test_cli_connections_export_should_export_as_yaml(self, tmp_path, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path / "connections.yaml"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args, cli_api_client=cli_api_client)
        assert output_filepath.read_text() == self.expected_connections_as_yaml

    @pytest.mark.parametrize(
        "serialization_format, expected",
        [
            (
                "uri",
                {
                    "airflow_db": "mysql://",
                    "google_cloud_default": "google-cloud-platform://",
                    "http_default": "http://",
                    "local_mysql": "mysql://",
                    "mongo_default": "mongo://",
                    "mssql_default": "mssql://",
                    "mysql_default": "mysql://",
                    "pinot_broker_default": "pinot://",
                    "postgres_default": "postgres://",
                    "presto_default": "presto://",
                    "sqlite_default": "sqlite://",
                    "trino_default": "trino://",
                    "vertica_default": "vertica://",
                },
            ),
            (
                None,  # tests that default is URI
                {
                    "airflow_db": "mysql://",
                    "google_cloud_default": "google-cloud-platform://",
                    "http_default": "http://",
                    "local_mysql": "mysql://",
                    "mongo_default": "mongo://",
                    "mssql_default": "mssql://",
                    "mysql_default": "mysql://",
                    "pinot_broker_default": "pinot://",
                    "postgres_default": "postgres://",
                    "presto_default": "presto://",
                    "sqlite_default": "sqlite://",
                    "trino_default": "trino://",
                    "vertica_default": "vertica://",
                },
            ),
            (
                "json",
                {
                    "airflow_db": {
                        "conn_type": "mysql",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "google_cloud_default": {
                        "conn_type": "google_cloud_platform",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "http_default": {
                        "conn_type": "http",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "local_mysql": {
                        "conn_type": "mysql",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "mongo_default": {
                        "conn_type": "mongo",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "mssql_default": {
                        "conn_type": "mssql",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "mysql_default": {
                        "conn_type": "mysql",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "pinot_broker_default": {
                        "conn_type": "pinot",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "postgres_default": {
                        "conn_type": "postgres",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "presto_default": {
                        "conn_type": "presto",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "sqlite_default": {
                        "conn_type": "sqlite",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "trino_default": {
                        "conn_type": "trino",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                    "vertica_default": {
                        "conn_type": "vertica",
                        "description": None,
                        "login": None,
                        "password": None,
                        "host": None,
                        "port": None,
                        "schema": None,
                        "extra": None,
                    },
                },
            ),
        ],
    )
    def test_cli_connections_export_should_export_as_env(
        self, serialization_format, expected, tmp_path, cli_api_client_maker
    ):
        """
        When exporting with env file format, we should
        """
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path / "connections.env"
        args_input = [
            "connections",
            "export",
            output_filepath.as_posix(),
        ]
        if serialization_format:
            args_input = [*args_input, "--serialization-format", serialization_format]
        args = self.parser.parse_args(args_input)
        connection_command.connections_export(args, cli_api_client=cli_api_client)
        assert output_filepath.read_text().splitlines() == [
            f"{key}={json.dumps(value) if type(value) is dict else value}" for key, value in expected.items()
        ]

    def test_cli_connections_export_should_export_as_env_for_uppercase_file_extension(
        self, tmp_path, cli_api_client_maker
    ):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path.absolute() / "connections.ENV"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args, cli_api_client=cli_api_client)
        assert output_filepath.read_text().splitlines() == self.expected_connections_as_env

    def test_cli_connections_export_should_force_export_as_specified_format(
        self, tmp_path, cli_api_client_maker
    ):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connections.model_dump(),
            expected_http_status_code=200,
        )
        output_filepath = tmp_path / "connections.yaml"
        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath.as_posix(),
                "--format",
                "json",
            ]
        )
        connection_command.connections_export(args, cli_api_client=cli_api_client)

        assert json.loads(output_filepath.read_text()) == self.expected_connections


TEST_URL = "postgresql://airflow:airflow@host:5432/airflow"
TEST_JSON = json.dumps(
    {
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "host",
        "port": 5432,
        "schema": "airflow",
        "description": "new0-json description",
    }
)


class TestCliAddConnections(TestCliConnection):
    @pytest.mark.parametrize(
        "cmd, expected_output, expected_conn",
        [
            pytest.param(
                [
                    "connections",
                    "add",
                    "new0-json",
                    f"--conn-json={TEST_JSON}",
                ],
                "Successfully added `conn_id`=new0-json : postgres://airflow:******@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new0-json description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": None,
                },
                id="json-connection",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new0",
                    f"--conn-uri={TEST_URL}",
                    "--conn-description=new0 description",
                ],
                "Successfully added `conn_id`=new0 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new0 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": None,
                },
                id="uri-connection-with-description",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new1",
                    f"--conn-uri={TEST_URL}",
                    "--conn-description=new1 description",
                ],
                "Successfully added `conn_id`=new1 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new1 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": None,
                },
                id="uri-connection-with-description-2",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new2",
                    f"--conn-uri={TEST_URL}",
                    "--conn-extra",
                    '{"extra": "yes"}',
                ],
                "Successfully added `conn_id`=new2 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": None,
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": '{"extra": "yes"}',
                },
                id="uri-connection-with-extra",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new3",
                    f"--conn-uri={TEST_URL}",
                    "--conn-extra",
                    '{"extra": "yes"}',
                    "--conn-description",
                    "new3 description",
                ],
                "Successfully added `conn_id`=new3 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new3 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": '{"extra": "yes"}',
                },
                id="uri-connection-with-extra-and-description",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new4",
                    "--conn-type=hive_metastore",
                    "--conn-login=airflow",
                    "--conn-password=airflow",
                    "--conn-host=host",
                    "--conn-port=9083",
                    "--conn-schema=airflow",
                    "--conn-description=  new4 description  ",
                ],
                "Successfully added `conn_id`=new4 : hive_metastore://airflow:******@host:9083/airflow",
                {
                    "conn_type": "hive_metastore",
                    "description": "  new4 description  ",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 9083,
                    "schema": "airflow",
                    "extra": None,
                },
                id="individual-parts",
            ),
            pytest.param(
                [
                    "connections",
                    "add",
                    "new5",
                    "--conn-uri",
                    "",
                    "--conn-type=google_cloud_platform",
                    "--conn-extra",
                    '{"extra": "yes"}',
                    "--conn-description=new5 description",
                ],
                "Successfully added `conn_id`=new5 : google_cloud_platform://:@:",
                {
                    "conn_type": "google_cloud_platform",
                    "description": "new5 description",
                    "host": None,
                    "is_encrypted": False,
                    "is_extra_encrypted": True,
                    "login": None,
                    "port": None,
                    "schema": None,
                    "extra": '{"extra": "yes"}',
                },
                id="empty-uri-with-conn-type-and-extra",
            ),
            pytest.param(
                ["connections", "add", "new6", "--conn-uri", "aws://?region_name=foo-bar-1"],
                "Successfully added `conn_id`=new6 : aws://?region_name=foo-bar-1",
                {
                    "conn_type": "aws",
                    "description": None,
                    "host": "",
                    "is_encrypted": False,
                    "is_extra_encrypted": True,
                    "login": None,
                    "port": None,
                    "schema": "",
                    "extra": '{"region_name": "foo-bar-1"}',
                },
                id="uri-without-authority-and-host-blocks",
            ),
            pytest.param(
                ["connections", "add", "new7", "--conn-uri", "aws://@/?region_name=foo-bar-1"],
                "Successfully added `conn_id`=new7 : aws://@/?region_name=foo-bar-1",
                {
                    "conn_type": "aws",
                    "description": None,
                    "host": "",
                    "is_encrypted": False,
                    "is_extra_encrypted": True,
                    "login": "",
                    "port": None,
                    "schema": "",
                    "extra": '{"region_name": "foo-bar-1"}',
                },
                id="uri-with-@-instead-authority-and-host-blocks",
            ),
        ],
    )
    @pytest.mark.execution_timeout(120)
    def test_cli_connection_add(self, cmd, expected_output, expected_conn: dict, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=ConnectionResponse(
                connection_id=cmd[2],
                conn_type=expected_conn["conn_type"],
                description=expected_conn["description"],
                host=expected_conn["host"],
                login=expected_conn["login"],
                schema_=expected_conn["schema"],
                port=expected_conn["port"],
                extra=expected_conn["extra"],
            ).model_dump(),
            expected_http_status_code=201,
        )
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_add(self.parser.parse_args(cmd), cli_api_client=cli_api_client)

        stdout_value = stdout.getvalue()

        assert expected_output in stdout_value

    def test_cli_connections_add_duplicate(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connection.model_dump(),
            expected_http_status_code=201,
        )
        connection_command.connections_add(
            self.parser.parse_args(
                ["connections", "add", self.connection.connection_id, f"--conn-uri={TEST_URL}"],
            ),
            cli_api_client=cli_api_client,
        )
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connection.model_dump(),
            expected_http_status_code=409,
        )
        # Check for addition attempt
        with pytest.raises(SystemExit):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", self.connection.connection_id, f"--conn-uri={TEST_URL}"],
                ),
                cli_api_client=cli_api_client,
            )

    def test_cli_connections_add_delete_with_missing_parameters(self):
        # Attempt to add without providing conn_uri
        with pytest.raises(
            SystemExit,
            match="Must supply either conn-uri or conn-json if not supplying conn-type",
        ):
            connection_command.connections_add(self.parser.parse_args(["connections", "add", "new1"]))

    def test_cli_connections_add_json_invalid_args(self):
        """can't supply extra and json"""
        with pytest.raises(
            SystemExit,
            match=r"The following args are not compatible with the --conn-json flag: \['--conn-extra'\]",
        ):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", "new1", f"--conn-json={TEST_JSON}", "--conn-extra='hi'"]
                )
            )

    def test_cli_connections_add_json_and_uri(self):
        """can't supply both uri and json"""
        with pytest.raises(
            SystemExit,
            match="Cannot supply both conn-uri and conn-json",
        ):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", "new1", f"--conn-uri={TEST_URL}", f"--conn-json={TEST_JSON}"]
                )
            )

    @pytest.mark.parametrize(
        "invalid_uri",
        [
            pytest.param("nonsense_uri", id="word"),
            pytest.param("://password:type@host:42/schema", id="missing-conn-type"),
        ],
    )
    def test_cli_connections_add_invalid_uri(self, invalid_uri):
        # Attempt to add with invalid uri
        with pytest.raises(SystemExit, match=r"The URI provided to --conn-uri is invalid: .*"):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", "new1", f"--conn-uri={shlex.quote(invalid_uri)}"]
                )
            )

    def test_cli_connections_add_invalid_type(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections",
            response_json=self.connection.model_dump(),
            expected_http_status_code=201,
        )
        with warnings.catch_warnings(record=True):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", "fsconn", "--conn-host=/tmp", "--conn-type=File"]
                ),
                cli_api_client=cli_api_client,
            )

    def test_cli_connections_add_invalid_conn_id(self):
        with pytest.raises(SystemExit) as e:
            connection_command.connections_add(
                self.parser.parse_args(["connections", "add", "Test$", f"--conn-uri={TEST_URL}"])
            )
        assert (
            e.value.args[0] == "Could not create connection. The key 'Test$' has to be made of "
            "alphanumeric characters, dashes, dots and underscores exclusively"
        )


class TestCliDeleteConnections(TestCliConnection):
    def test_cli_delete_connections(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path=f"/public/connections/{self.connection.connection_id}",
            response_json={"detail": "Successfully deleted connection with `conn_id`=new1"},
            expected_http_status_code=204,
        )
        # Delete connections
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_delete(
                self.parser.parse_args(["connections", "delete", self.connection.connection_id]),
                cli_api_client=cli_api_client,
            )
            stdout = stdout.getvalue()

        # Check deletion stdout
        assert f"Successfully deleted connection with `conn_id`={self.connection.connection_id}" in stdout

    def test_cli_delete_invalid_connection(self, cli_api_client_maker):
        # Attempt to delete a non-existing connection
        cli_api_client = cli_api_client_maker(
            path=f"/public/connections/{self.connection.connection_id}",
            response_json={"detail": "Connection not found."},
            expected_http_status_code=404,
        )
        with pytest.raises(SystemExit):
            connection_command.connections_delete(
                self.parser.parse_args(["connections", "delete", self.connection.connection_id]),
                cli_api_client=cli_api_client,
            )


class TestCliImportConnections(TestCliConnection):
    @mock.patch("os.path.exists")
    def test_cli_connections_import_should_return_error_if_file_does_not_exist(
        self, mock_exists, cli_api_client_maker
    ):
        mock_exists.side_effect = [True, False]
        filepath = "/does/not/exist.json"
        cli_api_client = cli_api_client_maker(
            path="dummy",
            response_json=self.connections.model_dump(),
            expected_http_status_code=201,
        )
        with pytest.raises(
            AirflowException,
            match=r"File (.*) was not found. Check the configuration of your Secrets backend.",
        ):
            connection_command.connections_import(
                self.parser.parse_args(["connections", "import", filepath]), cli_api_client=cli_api_client
            )

    @pytest.mark.parametrize("filepath", ["sample.jso", "sample.environ"])
    @mock.patch("os.path.exists")
    def test_cli_connections_import_should_return_error_if_file_format_is_invalid(
        self, mock_exists, filepath
    ):
        mock_exists.return_value = True
        with pytest.raises(
            AirflowException,
            match=(
                "Unsupported file format. The file must have one of the following extensions: "
                ".env .json .yaml .yml"
            ),
        ):
            connection_command.connections_import(self.parser.parse_args(["connections", "import", filepath]))

    @mock.patch("airflow.secrets.local_filesystem._parse_secret_file")
    @mock.patch("os.path.exists")
    def test_cli_connections_import_should_load_connections(
        self, mock_exists, mock_parse_secret_file, cli_api_client_maker
    ):
        mock_exists.return_value = True

        # We're not testing the behavior of _parse_secret_file, assume it successfully reads JSON, YAML or env
        mock_parse_secret_file.return_value = self.expected_connections

        cli_api_client = cli_api_client_maker(
            path="/public/connections/bulk",
            response_json=self.connections.model_dump(),
            expected_http_status_code=201,
        )

        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_import(
                self.parser.parse_args(["connections", "import", "sample.json"]),
                cli_api_client=cli_api_client,
            )

        stdout = stdout.getvalue()
        for connection_id, _ in self.expected_connections.items():
            assert f"Imported connection {connection_id}" in stdout

    @mock.patch("airflow.secrets.local_filesystem._parse_secret_file")
    @mock.patch("os.path.exists")
    def test_cli_connections_import_should_not_overwrite_existing_connections(
        self, mock_exists, mock_parse_secret_file, cli_api_client_maker
    ):
        mock_exists.return_value = True

        # We're not testing the behavior of _parse_secret_file, assume it successfully reads JSON, YAML or env
        mock_parse_secret_file.return_value = self.expected_connections

        cli_api_client = cli_api_client_maker(
            path="/public/connections/bulk",
            response_json={"detail": {"reason": "Unique constraint violation"}},
            expected_http_status_code=409,
        )

        with redirect_stdout(StringIO()) as stdout, pytest.raises(SystemExit):
            connection_command.connections_import(
                self.parser.parse_args(["connections", "import", "sample.json"]),
                cli_api_client=cli_api_client,
            )

        stdout = stdout.getvalue()
        assert "Unique constraint violation" in stdout

    @mock.patch("airflow.secrets.local_filesystem._parse_secret_file")
    @mock.patch("os.path.exists")
    def test_cli_connections_import_should_overwrite_existing_connections(
        self, mock_exists, mock_parse_secret_file, cli_api_client_maker
    ):
        mock_exists.return_value = True

        # We're not testing the behavior of _parse_secret_file, assume it successfully reads JSON, YAML or env
        mock_parse_secret_file.return_value = self.expected_connections

        cli_api_client = cli_api_client_maker(
            path="/public/connections/bulk",
            response_json=self.connections.model_dump(),
            expected_http_status_code=201,
        )

        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_import(
                self.parser.parse_args(["connections", "import", "sample.json", "--overwrite"]),
                cli_api_client=cli_api_client,
            )
            stdout = stdout.getvalue()

        for connection_id, _ in self.expected_connections.items():
            assert f"Imported connection {connection_id}" in stdout


class TestCliTestConnections(TestCliConnection):
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_cli_connections_test_success(self, cli_api_client_maker):
        """Check that successful connection test result is displayed properly."""
        cli_api_client = cli_api_client_maker(
            path="/public/connections/test",
            response_json=self.connection_test_response.model_dump(),
            expected_http_status_code=200,
        )
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_test(
                self.parser.parse_args(
                    ["connections", "test", self.connection.connection_id, self.connection.conn_type]
                ),
                cli_api_client=cli_api_client,
            )

            assert "Connection success!" in stdout.getvalue()

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_cli_connections_test_fail(self, cli_api_client_maker):
        """Check that failed connection test result is displayed properly."""
        cli_api_client = cli_api_client_maker(
            path="/public/connections/test",
            response_json=self.connection_failed_test_response.model_dump(),
            expected_http_status_code=200,
        )
        with redirect_stdout(StringIO()) as stdout:
            connection_command.connections_test(
                self.parser.parse_args(
                    ["connections", "test", self.connection.connection_id, self.connection.conn_type]
                ),
                cli_api_client=cli_api_client,
            )

            assert "Connection failed!" in stdout.getvalue()

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_cli_connections_test_missing_conn(self, cli_api_client_maker):
        """Check a connection test on a non-existent connection raises a "Connection not found" message."""
        cli_api_client = cli_api_client_maker(
            path="/public/connections/test",
            response_json={"detail": "Connection not found."},
            expected_http_status_code=404,
        )
        with redirect_stdout(StringIO()) as stdout, pytest.raises(SystemExit):
            connection_command.connections_test(
                self.parser.parse_args(
                    ["connections", "test", self.connection.connection_id, self.connection.conn_type]
                ),
                cli_api_client=cli_api_client,
            )
        assert "Connection not found." in stdout.getvalue()

    def test_cli_connections_test_disabled_by_default(self, cli_api_client_maker):
        """Check that test connection functionality is disabled by default."""
        cli_api_client = cli_api_client_maker(
            path="/public/connections/test",
            response_json=self.connection_test_response.model_dump(),
            expected_http_status_code=200,
        )
        with redirect_stdout(StringIO()) as stdout, pytest.raises(SystemExit):
            connection_command.connections_test(
                self.parser.parse_args(
                    ["connections", "test", self.connection.connection_id, self.connection.conn_type]
                ),
                cli_api_client=cli_api_client,
            )
        assert (
            "Testing connections is disabled in Airflow configuration. Contact your deployment admin to "
            "enable it.\n\n"
        ) in stdout.getvalue()


class TestCliCreateDefaultConnection(TestCliConnection):
    def test_cli_create_default_connections(self, cli_api_client_maker):
        cli_api_client = cli_api_client_maker(
            path="/public/connections/defaults",
            response_json="",
            expected_http_status_code=204,
        )

        with redirect_stdout(StringIO()) as stdout:
            connection_command.create_default_connections(
                self.parser.parse_args(["connections", "create-default-connections"]),
                cli_api_client=cli_api_client,
            )
            stdout = stdout.getvalue()

        assert "Default connections have been created." in stdout
