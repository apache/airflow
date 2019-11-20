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
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from airflow import settings
from airflow.bin import cli
from airflow.cli.commands import connection_command
from airflow.models import Connection
from airflow.utils import db


class TestCliConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    def test_cli_connections_list(self):
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            connection_command.connections_list(self.parser.parse_args(['connections', 'list']))
            stdout = mock_stdout.getvalue()
        conns = [[x.strip("'") for x in re.findall(r"'\w+'", line)[:2]]
                 for ii, line in enumerate(stdout.split('\n'))
                 if ii % 2 == 1]
        conns = [conn for conn in conns if len(conn) > 0]

        # Assert that some of the connections are present in the output as
        # expected:
        self.assertIn(['aws_default', 'aws'], conns)
        self.assertIn(['hive_cli_default', 'hive_cli'], conns)
        self.assertIn(['emr_default', 'emr'], conns)
        self.assertIn(['mssql_default', 'mssql'], conns)
        self.assertIn(['mysql_default', 'mysql'], conns)
        self.assertIn(['postgres_default', 'postgres'], conns)
        self.assertIn(['wasb_default', 'wasb'], conns)
        self.assertIn(['segment_default', 'segment'], conns)

    def test_cli_connections_list_with_args(self):
        args = self.parser.parse_args(['connections', 'list',
                                       '--output', 'tsv'])
        connection_command.connections_list(args)

    def test_cli_connections_list_redirect(self):
        cmd = ['airflow', 'connections', 'list']
        with tempfile.TemporaryFile() as file:
            proc = subprocess.Popen(cmd, stdout=file)
            proc.wait()
            self.assertEqual(0, proc.returncode)

    def test_cli_connections_add_delete(self):
        # TODO: We should not delete the entire database, but only reset the contents of the Connection table.
        db.resetdb()
        # Add connections:
        uri = 'postgresql://airflow:airflow@host:5432/airflow'
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new2',
                 '--conn_uri=%s' % uri]))
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new3',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new4',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new5',
                 '--conn_type=hive_metastore', '--conn_login=airflow',
                 '--conn_password=airflow', '--conn_host=host',
                 '--conn_port=9083', '--conn_schema=airflow']))
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new6',
                 '--conn_uri', "", '--conn_type=google_cloud_platform', '--conn_extra', "{'extra': 'yes'}"]))
            stdout = mock_stdout.getvalue()

        # Check addition stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tSuccessfully added `conn_id`=new1 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new2 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new3 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new4 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new5 : " +
             "hive_metastore://airflow:airflow@host:9083/airflow"),
            ("\tSuccessfully added `conn_id`=new6 : " +
             "google_cloud_platform://:@:")
        ])

        # Attempt to add duplicate
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tA connection with `conn_id`=new1 already exists",
        ])

        # Attempt to add without providing conn_uri
        with self.assertRaises(SystemExit) as exc:
            connection_command.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new']))

        self.assertEqual(
            exc.exception.code,
            "The following args are required to add a connection: ['conn_uri or conn_type']"
        )

        # Prepare to add connections
        session = settings.Session()
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'extra': 'yes'}",
                 'new4': "{'extra': 'yes'}"}

        # Add connections
        for index in range(1, 6):
            conn_id = 'new%s' % index
            result = (session
                      .query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            if conn_id in ['new1', 'new2', 'new3', 'new4']:
                self.assertEqual(result, (conn_id, 'postgres', 'host', 5432,
                                          extra[conn_id]))
            elif conn_id == 'new5':
                self.assertEqual(result, (conn_id, 'hive_metastore', 'host',
                                          9083, None))
            elif conn_id == 'new6':
                self.assertEqual(result, (conn_id, 'google_cloud_platform',
                                          None, None, "{'extra': 'yes'}"))

        # Delete connections
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new1']))
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new2']))
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new3']))
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new4']))
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new5']))
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new6']))
            stdout = mock_stdout.getvalue()

        # Check deletion stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tSuccessfully deleted `conn_id`=new1",
            "\tSuccessfully deleted `conn_id`=new2",
            "\tSuccessfully deleted `conn_id`=new3",
            "\tSuccessfully deleted `conn_id`=new4",
            "\tSuccessfully deleted `conn_id`=new5",
            "\tSuccessfully deleted `conn_id`=new6"
        ])

        # Check deletions
        for index in range(1, 7):
            conn_id = 'new%s' % index
            result = (session.query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())

            self.assertTrue(result is None)

        # Attempt to delete a non-existing connection
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            connection_command.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'fake']))
            stdout = mock_stdout.getvalue()

        # Check deletion attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tDid not find a connection with `conn_id`=fake",
        ])

        session.close()
