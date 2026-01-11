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
import shlex
import warnings
from contextlib import redirect_stdout
from io import StringIO

import pytest
from sqlalchemy import select

from airflow.cli import cli_config, cli_parser
from airflow.cli.commands import connection_command
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_connections
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module", autouse=True)
def clear_connections():
    yield
    clear_db_connections(add_default_connections_back=False)


class TestCliGetConnection:
    parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_connections(add_default_connections_back=True)

    def test_cli_connection_get(self, stdout_capture):
        with stdout_capture as capture:
            connection_command.connections_get(
                self.parser.parse_args(["connections", "get", "google_cloud_default", "--output", "json"])
            )
        assert "google-cloud-platform:///default" in capture.getvalue()

    def test_cli_connection_get_invalid(self):
        with pytest.raises(SystemExit, match=re.escape("Connection not found.")):
            connection_command.connections_get(self.parser.parse_args(["connections", "get", "INVALID"]))


class TestCliListConnections:
    parser = cli_parser.get_parser()
    EXPECTED_CONS = [
        ("airflow_db", "mysql"),
        ("google_cloud_default", "google_cloud_platform"),
        ("http_default", "http"),
        ("local_mysql", "mysql"),
        ("mongo_default", "mongo"),
        ("mssql_default", "mssql"),
        ("mysql_default", "mysql"),
        ("pinot_broker_default", "pinot"),
        ("postgres_default", "postgres"),
        ("presto_default", "presto"),
        ("sqlite_default", "sqlite"),
        ("trino_default", "trino"),
        ("vertica_default", "vertica"),
    ]

    def setup_method(self):
        clear_db_connections(add_default_connections_back=True)

    def test_cli_connections_list_as_json(self):
        args = self.parser.parse_args(["connections", "list", "--output", "json"])
        with redirect_stdout(StringIO()) as stdout_io:
            connection_command.connections_list(args)
            print(stdout_io.getvalue())
            stdout = stdout_io.getvalue()
        for conn_id, conn_type in self.EXPECTED_CONS:
            assert conn_type in stdout
            assert conn_id in stdout


class TestCliExportConnections:
    parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_connections(add_default_connections_back=False)
        merge_conn(
            Connection(
                conn_id="airflow_db",
                conn_type="mysql",
                description="mysql conn description",
                host="mysql",
                login="root",
                password="plainpassword",
                schema="airflow",
            ),
        )
        merge_conn(
            Connection(
                conn_id="druid_broker_default",
                conn_type="druid",
                description="druid-broker conn description",
                host="druid-broker",
                port=8082,
                extra='{"endpoint": "druid/v2/sql"}',
            ),
        )

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

    def test_cli_connections_export_should_raise_error_if_create_session_fails(self, mocker, tmp_path):
        output_filepath = tmp_path / "connections.json"
        mocker.patch.object(connection_command, "create_session", side_effect=Exception("dummy exception"))
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        with pytest.raises(Exception, match=r"dummy exception"):
            connection_command.connections_export(args)

    def test_cli_connections_export_should_raise_error_if_fetching_connections_fails(self, mocker, tmp_path):
        output_filepath = tmp_path / "connections.json"
        mock_session = mocker.patch.object(connection_command, "create_session")
        mock_session.return_value.__enter__.return_value.scalars.side_effect = Exception("dummy exception")
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        with pytest.raises(Exception, match=r"dummy exception"):
            connection_command.connections_export(args)

    def test_cli_connections_export_should_not_raise_error_if_connections_is_empty(self, mocker, tmp_path):
        output_filepath = tmp_path / "connections.json"
        mock_session = mocker.patch.object(connection_command, "create_session")
        mock_session.return_value.__enter__.return_value.query.return_value.all.return_value = []
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args)
        assert output_filepath.read_text() == "{}"

    def test_cli_connections_export_should_export_as_json(self, tmp_path):
        output_filepath = tmp_path / "connections.json"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args)
        expected_connections = {
            "airflow_db": {
                "conn_type": "mysql",
                "description": "mysql conn description",
                "host": "mysql",
                "login": "root",
                "password": "plainpassword",
                "schema": "airflow",
                "port": None,
                "extra": None,
            },
            "druid_broker_default": {
                "conn_type": "druid",
                "description": "druid-broker conn description",
                "host": "druid-broker",
                "login": None,
                "password": None,
                "schema": None,
                "port": 8082,
                "extra": '{"endpoint": "druid/v2/sql"}',
            },
        }
        assert json.loads(output_filepath.read_text()) == expected_connections

    def test_cli_connections_export_should_export_as_yaml(self, tmp_path):
        output_filepath = tmp_path / "connections.yaml"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args)
        expected_connections = (
            "airflow_db:\n"
            "  conn_type: mysql\n"
            "  description: mysql conn description\n"
            "  extra: null\n"
            "  host: mysql\n"
            "  login: root\n"
            "  password: plainpassword\n"
            "  port: null\n"
            "  schema: airflow\n"
            "druid_broker_default:\n"
            "  conn_type: druid\n"
            "  description: druid-broker conn description\n"
            '  extra: \'{"endpoint": "druid/v2/sql"}\'\n'
            "  host: druid-broker\n"
            "  login: null\n"
            "  password: null\n"
            "  port: 8082\n"
            "  schema: null\n"
        )
        assert output_filepath.read_text() == expected_connections

    @pytest.mark.parametrize(
        ("serialization_format", "expected"),
        [
            (
                "uri",
                [
                    "airflow_db=mysql://root:plainpassword@mysql/airflow",
                    "druid_broker_default=druid://druid-broker:8082/?endpoint=druid%2Fv2%2Fsql",
                ],
            ),
            (
                None,  # tests that default is URI
                [
                    "airflow_db=mysql://root:plainpassword@mysql/airflow",
                    "druid_broker_default=druid://druid-broker:8082/?endpoint=druid%2Fv2%2Fsql",
                ],
            ),
            (
                "json",
                [
                    'airflow_db={"conn_type": "mysql", "description": "mysql conn description", '
                    '"login": "root", "password": "plainpassword", "host": "mysql", "port": null, '
                    '"schema": "airflow", "extra": null}',
                    'druid_broker_default={"conn_type": "druid", "description": "druid-broker conn '
                    'description", "login": null, "password": null, "host": "druid-broker", "port": 8082, '
                    '"schema": null, "extra": "{\\"endpoint\\": \\"druid/v2/sql\\"}"}',
                ],
            ),
        ],
    )
    def test_cli_connections_export_should_export_as_env(self, serialization_format, expected, tmp_path):
        """
        When exporting with env file format, we should
        """
        output_filepath = tmp_path / "connections.env"
        args_input = [
            "connections",
            "export",
            output_filepath.as_posix(),
        ]
        if serialization_format:
            args_input = [*args_input, "--serialization-format", serialization_format]
        args = self.parser.parse_args(args_input)
        connection_command.connections_export(args)
        assert output_filepath.read_text().splitlines() == expected

    def test_cli_connections_export_should_export_as_env_for_uppercase_file_extension(self, tmp_path):
        output_filepath = tmp_path / "connections.ENV"
        args = self.parser.parse_args(["connections", "export", output_filepath.as_posix()])
        connection_command.connections_export(args)
        expected_connections = [
            "airflow_db=mysql://root:plainpassword@mysql/airflow",
            "druid_broker_default=druid://druid-broker:8082/?endpoint=druid%2Fv2%2Fsql",
        ]

        assert output_filepath.read_text().splitlines() == expected_connections

    def test_cli_connections_export_should_force_export_as_specified_format(self, tmp_path):
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
        connection_command.connections_export(args)
        expected_connections = {
            "airflow_db": {
                "conn_type": "mysql",
                "description": "mysql conn description",
                "host": "mysql",
                "login": "root",
                "password": "plainpassword",
                "schema": "airflow",
                "port": None,
                "extra": None,
            },
            "druid_broker_default": {
                "conn_type": "druid",
                "description": "druid-broker conn description",
                "host": "druid-broker",
                "login": None,
                "password": None,
                "schema": None,
                "port": 8082,
                "extra": '{"endpoint": "druid/v2/sql"}',
            },
        }
        assert json.loads(output_filepath.read_text()) == expected_connections

    def test_cli_connections_export_should_work_when_stdout_is_not_a_real_fd(self, tmp_path):
        class FakeFileStringIO(StringIO):
            """
            Buffer the contents of a StringIO
            to make them accessible after close
            """

            def __init__(self):
                super().__init__()
                self.content = ""

            def write(self, s: str) -> int:
                self.content += s
                return len(s)

        tmp_stdout = FakeFileStringIO()
        with redirect_stdout(tmp_stdout):
            args = self.parser.parse_args(
                [
                    "connections",
                    "export",
                    "--format",
                    "env",
                    "-",
                ]
            )
            connection_command.connections_export(args)
        assert tmp_stdout.content.splitlines() == [
            "airflow_db=mysql://root:plainpassword@mysql/airflow",
            "druid_broker_default=druid://druid-broker:8082/?endpoint=druid%2Fv2%2Fsql",
        ]


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


class TestCliAddConnections:
    parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_connections(add_default_connections_back=False)

    @skip_if_force_lowest_dependencies_marker
    @pytest.mark.parametrize(
        ("cmd", "expected_output", "expected_conn"),
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
            pytest.param(
                ["connections", "add", "invalid-uri-test", "--conn-uri", "invalid_uri"],
                "The URI provided to --conn-uri is invalid: invalid_uri",
                None,  # No connection should be created
                id="invalid-uri",
            ),
        ],
    )
    @pytest.mark.execution_timeout(120)
    def test_cli_connection_add(self, cmd, expected_output, expected_conn, session, stdout_capture):
        if "invalid-uri-test" in cmd:
            with pytest.raises(SystemExit) as exc_info:
                connection_command.connections_add(self.parser.parse_args(cmd))

            assert str(exc_info.value) == expected_output
            return

        with stdout_capture as stdout:
            connection_command.connections_add(self.parser.parse_args(cmd))

        stdout = stdout.getvalue()

        assert expected_output in stdout
        conn_id = cmd[2]
        comparable_attrs = [
            "conn_type",
            "description",
            "host",
            "is_encrypted",
            "is_extra_encrypted",
            "login",
            "port",
            "schema",
            "extra",
        ]
        current_conn = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
        assert expected_conn == {attr: getattr(current_conn, attr) for attr in comparable_attrs}

    def test_cli_connections_add_duplicate(self):
        conn_id = "to_be_duplicated"
        connection_command.connections_add(
            self.parser.parse_args(["connections", "add", conn_id, f"--conn-uri={TEST_URL}"])
        )
        # Check for addition attempt
        with pytest.raises(SystemExit, match=rf"A connection with `conn_id`={conn_id} already exists"):
            connection_command.connections_add(
                self.parser.parse_args(["connections", "add", conn_id, f"--conn-uri={TEST_URL}"])
            )

    def test_cli_connections_add_delete_with_missing_parameters(self):
        # Attempt to add without providing conn_uri
        with pytest.raises(
            SystemExit,
            match="Must supply either conn-uri or conn-json if not supplying conn-type",
        ):
            connection_command.connections_add(self.parser.parse_args(["connections", "add", "new1"]))

    def test_cli_connections_add_json_invalid_args(self):
        """Can't supply extra and json"""
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
        """Can't supply both uri and json"""
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

    def test_cli_connections_add_invalid_type(self):
        with warnings.catch_warnings(record=True):
            connection_command.connections_add(
                self.parser.parse_args(
                    ["connections", "add", "fsconn", "--conn-host=/tmp", "--conn-type=File"]
                )
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


class TestCliDeleteConnections:
    parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_connections(add_default_connections_back=False)

    def test_cli_delete_connections(self, session, stdout_capture):
        merge_conn(
            Connection(
                conn_id="new1",
                conn_type="mysql",
                description="mysql description",
                host="mysql",
                login="root",
                password="",
                schema="airflow",
            ),
            session=session,
        )
        # Delete connections
        with stdout_capture as stdout:
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "new1"]))

        # Check deletion stdout
        assert "Successfully deleted connection with `conn_id`=new1" in stdout.getvalue()

        # Check deletions
        result = session.scalar(select(Connection).where(Connection.conn_id == "new1"))

        assert result is None

    def test_cli_delete_invalid_connection(self):
        # Attempt to delete a non-existing connection
        with pytest.raises(SystemExit, match=r"Did not find a connection with `conn_id`=fake"):
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "fake"]))


class TestCliImportConnections:
    parser = cli_parser.get_parser()

    def setup_method(self):
        clear_db_connections(add_default_connections_back=False)

    def test_cli_connections_import_should_return_error_if_file_does_not_exist(self, mocker):
        mocker.patch("os.path.exists", return_value=False)
        filepath = "/does/not/exist.json"
        with pytest.raises(SystemExit, match=r"Missing connections file."):
            connection_command.connections_import(self.parser.parse_args(["connections", "import", filepath]))

    @pytest.mark.parametrize("filepath", ["sample.jso", "sample.environ"])
    def test_cli_connections_import_should_return_error_if_file_format_is_invalid(self, filepath, mocker):
        mocker.patch("os.path.exists", return_value=True)
        with pytest.raises(
            AirflowException,
            match=(
                "Unsupported file format. The file must have one of the following extensions: "
                ".env .json .yaml .yml"
            ),
        ):
            connection_command.connections_import(self.parser.parse_args(["connections", "import", filepath]))

    def test_cli_connections_import_should_load_connections(self, mocker):
        # Sample connections to import
        expected_connections = {
            "new0": {
                "conn_type": "postgres",
                "description": "new0 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 5432,
                "schema": "airflow",
                "extra": '{"foo": "bar"}',
            },
            "new1": {
                "conn_type": "mysql",
                "description": "new1 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 3306,
                "schema": "airflow",
                "extra": '{"spam": "egg"}',
            },
            # Add new3 if the test expects an error about it
            "new3": {
                "conn_type": "sqlite",
                "description": "new3 description",
                "host": "host",
            },
        }

        # First, create new3 to trigger the "already exists" error
        with create_session() as session:
            session.add(Connection(conn_id="new3", conn_type="sqlite"))
            session.commit()

        # We're not testing the behavior of _parse_secret_file
        mocker.patch("airflow.secrets.local_filesystem._parse_secret_file", return_value=expected_connections)
        mocker.patch("os.path.exists", return_value=True)
        mock_print = mocker.patch("airflow.cli.commands.connection_command.print")

        connection_command.connections_import(
            self.parser.parse_args(["connections", "import", "sample.json"])
        )

        # Check all print calls to find the error message
        print_calls = [str(call) for call in mock_print.call_args_list]
        assert any("Could not import connection new3" in call for call in print_calls), (
            f"Expected error message not found. Print calls: {print_calls}"
        )

        # Verify connections (exclude new3 since it should fail)
        expected_imported = {k: v for k, v in expected_connections.items() if k != "new3"}

        with create_session() as session:
            current_conns = session.scalars(
                select(Connection).where(Connection.conn_id.in_(["new0", "new1"]))
            ).all()

            comparable_attrs = [
                "conn_id",
                "conn_type",
                "description",
                "host",
                "login",
                "password",
                "port",
                "schema",
                "extra",
            ]

            current_conns_as_dicts = {
                current_conn.conn_id: {attr: getattr(current_conn, attr) for attr in comparable_attrs}
                for current_conn in current_conns
            }
            assert expected_imported == current_conns_as_dicts

    def test_cli_connections_import_should_not_overwrite_existing_connections(self, session, mocker):
        mocker.patch("os.path.exists", return_value=True)
        # Add a pre-existing connection "new3"
        merge_conn(
            Connection(
                conn_id="new3",
                conn_type="mysql",
                description="original description",
                host="mysql",
                login="root",
                password="password",
                schema="airflow",
            ),
            session=session,
        )

        # Sample connections to import, including a collision with "new3"
        expected_connections = {
            "new2": {
                "conn_type": "postgres",
                "description": "new2 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 5432,
                "schema": "airflow",
                "extra": '{"foo": "bar"}',
            },
            "new3": {
                "conn_type": "mysql",
                "description": "updated description",
                "host": "host",
                "login": "airflow",
                "password": "new password",
                "port": 3306,
                "schema": "airflow",
                "extra": '{"spam": "egg"}',
            },
        }

        # We're not testing the behavior of _parse_secret_file, assume it successfully reads JSON, YAML or env
        mocker.patch("airflow.secrets.local_filesystem._parse_secret_file", return_value=expected_connections)
        mock_print = mocker.patch("airflow.cli.commands.connection_command.print")
        connection_command.connections_import(
            self.parser.parse_args(["connections", "import", "sample.json"])
        )
        assert "Could not import connection new3: connection already exists." in mock_print.call_args[0][0]

        # Verify that the imported connections match the expected, sample connections
        current_conns = session.scalars(select(Connection)).all()

        comparable_attrs = [
            "conn_id",
            "conn_type",
            "description",
            "host",
            "login",
            "password",
            "port",
            "schema",
            "extra",
        ]

        current_conns_as_dicts = {
            current_conn.conn_id: {attr: getattr(current_conn, attr) for attr in comparable_attrs}
            for current_conn in current_conns
        }
        assert current_conns_as_dicts["new2"] == expected_connections["new2"]

        # The existing connection's description should not have changed
        assert current_conns_as_dicts["new3"]["description"] == "original description"

    def test_cli_connections_import_should_overwrite_existing_connections(self, mocker, session):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch(
            "airflow.secrets.local_filesystem._parse_secret_file",
            return_value={
                "new2": {
                    "conn_type": "postgres",
                    "description": "new2 description",
                    "host": "host",
                    "login": "airflow",
                    "password": "password",
                    "port": 5432,
                    "schema": "airflow",
                    "extra": '{"foo": "bar"}',
                },
                "new3": {
                    "conn_type": "mysql",
                    "description": "updated description",
                    "host": "host",
                    "login": "airflow",
                    "password": "new password",
                    "port": 3306,
                    "schema": "airflow",
                    "extra": '{"spam": "egg"}',
                },
            },
        )
        # Add a pre-existing connection "new3"
        merge_conn(
            Connection(
                conn_id="new3",
                conn_type="mysql",
                description="original description",
                host="mysql",
                login="root",
                password="password",
                schema="airflow",
            ),
            session=session,
        )
        mock_print = mocker.patch("airflow.cli.commands.connection_command.print")
        connection_command.connections_import(
            self.parser.parse_args(["connections", "import", "sample.json", "--overwrite"])
        )
        assert (
            "Could not import connection new3: connection already exists." not in mock_print.call_args[0][0]
        )
        # Verify that the imported connections match the expected, sample connections
        current_conns = session.scalars(select(Connection)).all()
        comparable_attrs = [
            "conn_id",
            "conn_type",
            "description",
            "host",
            "login",
            "password",
            "port",
            "schema",
            "extra",
        ]
        current_conns_as_dicts = {
            current_conn.conn_id: {attr: getattr(current_conn, attr) for attr in comparable_attrs}
            for current_conn in current_conns
        }
        assert current_conns_as_dicts["new2"] == {
            "conn_type": "postgres",
            "description": "new2 description",
            "host": "host",
            "login": "airflow",
            "password": "password",
            "port": 5432,
            "schema": "airflow",
            "extra": '{"foo": "bar"}',
            "conn_id": "new2",
        }
        # The existing connection should have been overwritten
        assert current_conns_as_dicts["new3"] == {
            "conn_type": "mysql",
            "description": "updated description",
            "host": "host",
            "login": "airflow",
            "password": "new password",
            "port": 3306,
            "schema": "airflow",
            "extra": '{"spam": "egg"}',
            "conn_id": "new3",
        }


class TestCliTestConnections:
    parser = cli_parser.get_parser()

    def setup_class(self):
        clear_db_connections()

    def test_cli_connections_test_success(self, mocker, stdout_capture):
        mocker.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
        mock_test_conn = mocker.patch("airflow.providers.http.hooks.http.HttpHook.test_connection")
        conn_id = "http_default"
        mock_test_conn.return_value = True, None
        with stdout_capture as stdout:
            connection_command.connections_test(self.parser.parse_args(["connections", "test", conn_id]))
            assert "Connection success!" in stdout.getvalue()

    def test_cli_connections_test_fail(self, mocker, stdout_capture):
        mocker.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
        mock_test_conn = mocker.patch("airflow.providers.http.hooks.http.HttpHook.test_connection")
        conn_id = "http_default"
        mock_test_conn.return_value = False, "Failed."
        with stdout_capture as stdout:
            connection_command.connections_test(self.parser.parse_args(["connections", "test", conn_id]))
            assert "Connection failed!\nFailed.\n\n" in stdout.getvalue()

    def test_cli_connections_test_missing_conn(self, mocker, stdout_capture):
        mocker.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
        with stdout_capture as stdout, pytest.raises(SystemExit):
            connection_command.connections_test(self.parser.parse_args(["connections", "test", "missing"]))
        assert "Connection not found.\n\n" in stdout.getvalue()

    def test_cli_connections_test_disabled_by_default(self, stdout_capture):
        with stdout_capture as stdout, pytest.raises(SystemExit):
            connection_command.connections_test(self.parser.parse_args(["connections", "test", "missing"]))
        assert (
            "Testing connections is disabled in Airflow configuration. Contact your deployment admin to "
            "enable it.\n\n"
        ) in stdout.getvalue()


class TestCliCreateDefaultConnection:
    def test_cli_create_default_connections(self, mocker):
        mock_db_create_default_connections = mocker.patch(
            "airflow.cli.commands.connection_command.db_create_default_connections"
        )
        create_default_connection_fnc = dict(
            (db_command.name, db_command.func) for db_command in cli_config.CONNECTIONS_COMMANDS
        )["create-default-connections"]
        create_default_connection_fnc(())
        mock_db_create_default_connections.assert_called_once()
