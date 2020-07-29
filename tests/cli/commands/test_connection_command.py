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

import io
import json
import pytest
import unittest
from contextlib import redirect_stdout, contextmanager
from unittest import mock

from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import connection_command
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow.utils.session import create_session, provide_session
from tests.test_utils.db import clear_db_connections


@contextmanager
def mock_local_file(content):
    with mock.patch(
        "airflow.secrets.local_filesystem.open", mock.mock_open(read_data=content)
    ) as file_mock, mock.patch("airflow.secrets.local_filesystem.os.path.exists", return_value=True):
        yield file_mock

class TestCliListConnections(unittest.TestCase):
    EXPECTED_CONS = [
        ('airflow_db', 'mysql', ),
        ('google_cloud_default', 'google_cloud_platform', ),
        ('http_default', 'http', ),
        ('local_mysql', 'mysql', ),
        ('mongo_default', 'mongo', ),
        ('mssql_default', 'mssql', ),
        ('mysql_default', 'mysql', ),
        ('pinot_broker_default', 'pinot', ),
        ('postgres_default', 'postgres', ),
        ('presto_default', 'presto', ),
        ('sqlite_default', 'sqlite', ),
        ('vertica_default', 'vertica', ),
    ]

    def setUp(self):
        self.parser = cli_parser.get_parser()
        clear_db_connections()

    def tearDown(self):
        clear_db_connections()

    def test_cli_connections_list(self):
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(self.parser.parse_args(["connections", "list"]))
            stdout = stdout.getvalue()
            lines = stdout.split("\n")

        for conn_id, conn_type in self.EXPECTED_CONS:
            self.assertTrue(any(conn_id in line and conn_type in line for line in lines))

    def test_cli_connections_list_as_tsv(self):
        args = self.parser.parse_args(["connections", "list", "--output", "tsv"])

        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(args)
            stdout = stdout.getvalue()
            lines = stdout.split("\n")

        for conn_id, conn_type in self.EXPECTED_CONS:
            self.assertTrue(any(conn_id in line and conn_type in line for line in lines))

    def test_cli_connections_filter_conn_id(self):
        args = self.parser.parse_args(["connections", "list", "--output", "tsv", '--conn-id', 'http_default'])

        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(args)
            stdout = stdout.getvalue()
            lines = stdout.split("\n")

        conn_ids = [line.split("\t", 2)[0].strip() for line in lines[1:] if line]
        self.assertEqual(conn_ids, ['http_default'])

    @mock.patch('airflow.cli.commands.connection_command.BaseHook.get_connections', return_value=[
        Connection(conn_id="http_default", host="host1"),
        Connection(conn_id="http_default", host="host2"),
    ])
    def test_cli_connections_filter_conn_id_include_secrets(self, mock_get_connections):
        args = self.parser.parse_args([
            "connections", "list", "--output", "tsv", '--conn-id', 'http_default', '--include-secrets'
        ])

        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(args)
            stdout = stdout.getvalue()
            lines = stdout.split("\n")

        conn_ids = [line.split("\t", 2)[0].strip() for line in lines[1:] if line]
        self.assertEqual(conn_ids, ['http_default', 'http_default'])
        mock_get_connections.assert_called_once_with('http_default')

    def test_cli_connections_include_secrets(self):
        args = self.parser.parse_args([
            "connections", "list", "--output", "tsv", '--include-secrets',
        ])

        with self.assertRaises(SystemExit):
            connection_command.connections_list(args)


TEST_URL = "postgresql://airflow:airflow@host:5432/airflow"


class TestCliAddConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections()

    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @parameterized.expand(
        [
            (
                ["connections", "add", "new0", "--conn-uri=%s" % TEST_URL],
                "\tSuccessfully added `conn_id`=new0 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                ["connections", "add", "new1", "--conn-uri=%s" % TEST_URL],
                "\tSuccessfully added `conn_id`=new1 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new2",
                    "--conn-uri=%s" % TEST_URL,
                    "--conn-extra",
                    "{'extra': 'yes'}",
                ],
                "\tSuccessfully added `conn_id`=new2 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new3",
                    "--conn-uri=%s" % TEST_URL,
                    "--conn-extra",
                    "{'extra': 'yes'}",
                ],
                "\tSuccessfully added `conn_id`=new3 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
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
                ],
                "\tSuccessfully added `conn_id`=new4 : hive_metastore://airflow:******@host:9083/airflow",
                {
                    "conn_type": "hive_metastore",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 9083,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new5",
                    "--conn-uri",
                    "",
                    "--conn-type=google_cloud_platform",
                    "--conn-extra",
                    "{'extra': 'yes'}",
                ],
                "\tSuccessfully added `conn_id`=new5 : google_cloud_platform://:@:",
                {
                    "conn_type": "google_cloud_platform",
                    "host": None,
                    "is_encrypted": False,
                    "is_extra_encrypted": True,
                    "login": None,
                    "port": None,
                    "schema": None,
                },
            ),
        ]
    )
    def test_cli_connection_add(self, cmd, expected_output, expected_conn):
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_add(self.parser.parse_args(cmd))

        stdout = stdout.getvalue()

        self.assertIn(expected_output, stdout)
        conn_id = cmd[2]
        with create_session() as session:
            comparable_attrs = [
                "conn_type",
                "host",
                "is_encrypted",
                "is_extra_encrypted",
                "login",
                "port",
                "schema",
            ]
            current_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            self.assertEqual(expected_conn, {attr: getattr(current_conn, attr) for attr in comparable_attrs})

    def test_cli_connections_add_duplicate(self):
        # Attempt to add duplicate
        connection_command.connections_add(
            self.parser.parse_args(["connections", "add", "new1", "--conn-uri=%s" % TEST_URL])
        )
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_add(
                self.parser.parse_args(["connections", "add", "new1", "--conn-uri=%s" % TEST_URL])
            )
            stdout = stdout.getvalue()

        # Check stdout for addition attempt
        self.assertIn("\tA connection with `conn_id`=new1 already exists", stdout)

    def test_cli_connections_add_delete_with_missing_parameters(self):
        # Attempt to add without providing conn_uri
        with self.assertRaisesRegex(
            SystemExit, r"The following args are required to add a connection: \['conn-uri or conn-type'\]"
        ):
            connection_command.connections_add(self.parser.parse_args(["connections", "add", "new1"]))


class TestCliDeleteConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections()

    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @provide_session
    def test_cli_delete_connections(self, session=None):
        merge_conn(
            Connection(
                conn_id="new1", conn_type="mysql", host="mysql", login="root", password="", schema="airflow"
            ),
            session=session
        )
        # Delete connections
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "new1"]))
            stdout = stdout.getvalue()

        # Check deletion stdout
        self.assertIn("\tSuccessfully deleted `conn_id`=new1", stdout)

        # Check deletions
        result = session.query(Connection).filter(Connection.conn_id == "new1").first()

        self.assertTrue(result is None)

    def test_cli_delete_invalid_connection(self):
        # Attempt to delete a non-existing connection
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "fake"]))
            stdout = stdout.getvalue()

        # Check deletion attempt stdout
        self.assertIn("\tDid not find a connection with `conn_id`=fake", stdout)

class TestCliImportConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections()
    
    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @parameterized.expand(
        (
            ({"CONN_ID": {'conn_type': 'mysql', 'host': 'host_1'}}, {"CONN_ID": {'conn_type': 'mysql', 'host': 'host_1'}}),
        )
    )
    def test_connections_import(self, file_content, expected_connection_uris):
        """Test connections_import command"""
    
        with mock_local_file(json.dumps(file_content)):
            connection_command.connections_import(self.parser.parse_args([
                     'connections', 'import', "a.json"]))
            with create_session() as session:
                for conn_id in expected_connection_uris:
                    current_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                    self.assertEqual(expected_connection_uris[conn_id], {attr: getattr(current_conn, attr) for attr in expected_connection_uris[conn_id]})

    @parameterized.expand(
        (
            (
                {"CONN_ID": {'conn_type': 'mysql', 'host': 'host_1'}},
                {"CONN_ID": {'conn_type': 'mysql', 'host': 'host_1'}}
            ),
        )
    )    
    def test_connections_import_disposition_restrict(self, file_content, expected_connection_uris):
        """Test connections_import command with --conflict-disposition restrict"""
        with pytest.raises(AirflowException, match='Connections with `conn_ids`=CONN_ID already exists\n') as e:
            with mock_local_file(json.dumps(file_content)):
                connection_command.connections_import(self.parser.parse_args([
                        'connections', 'import', "a.json"]))

                connection_command.connections_import(self.parser.parse_args([
                            'connections', 'import', "a.json"]))

    
    @parameterized.expand(
        (
            (
                {"CONN_ID2": [{'conn_type': 'mysql', 'host': 'host_1'},{'conn_type': 'mysql', 'host': 'host_2'}]},
                {"CONN_ID2": [{'conn_type': 'mysql', 'host': 'host_1'},{'conn_type': 'mysql', 'host': 'host_2'}]}
            ),
        )
    )    
    def test_connections_import_disposition_ignore(self, file_content, expected_connection_uris):
        """Test connections_import command with --conflict-disposition ignore"""
        with mock_local_file(json.dumps(file_content)):
            connection_command.connections_import(self.parser.parse_args([
                    'connections', 'import', 'a.json']))

            connection_command.connections_import(self.parser.parse_args([
                            'connections', 'import', 'a.json', '--conflict-disposition', 'ignore']))
            
            conn_id = 'CONN_ID2'
            with create_session() as session:
                current_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                current_conn_obj = {attr: getattr(current_conn, attr) for attr in expected_connection_uris[conn_id][0]}
                self.assertEqual(expected_connection_uris[conn_id][0], {attr: getattr(current_conn, attr) for attr in expected_connection_uris[conn_id][0]})


    @parameterized.expand(
        (
            (
                {"CONN_ID3": [{'conn_type': 'mysql', 'host': 'host_1'},{'conn_type': 'mysql', 'host': 'host_2'}]},
                {"CONN_ID3": [{'conn_type': 'mysql', 'host': 'host_1'},{'conn_type': 'mysql', 'host': 'host_2'}]}
            ),
        )
    )    
    def test_connections_import_disposition_overwrite(self, file_content, expected_connection_uris):
        """Test connections_import command with --conflict-disposition overwrite"""
        with mock_local_file(json.dumps(file_content)):
           
            connection_command.connections_import(self.parser.parse_args([
                            'connections', 'import', 'a.json', '--conflict-disposition', 'overwrite']))
 
            conn_id = 'CONN_ID3'
            with create_session() as session:
                current_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                current_conn_obj = {attr: getattr(current_conn, attr) for attr in expected_connection_uris[conn_id][0]}
                self.assertEqual(expected_connection_uris[conn_id][1], {attr: getattr(current_conn, attr) for attr in expected_connection_uris[conn_id][1]})