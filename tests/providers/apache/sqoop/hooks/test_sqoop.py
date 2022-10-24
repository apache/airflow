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

import collections
import json
import unittest
from io import StringIO
from unittest import mock
from unittest.mock import call, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.apache.sqoop.hooks.sqoop import SqoopHook
from airflow.utils import db


class TestSqoopHook(unittest.TestCase):
    _config = {
        "conn_id": "sqoop_test",
        "num_mappers": 22,
        "verbose": True,
        "properties": {"mapred.map.max.attempts": "1"},
        "hcatalog_database": "hive_database",
        "hcatalog_table": "hive_table",
    }
    _config_export = {
        "table": "export_data_to",
        "export_dir": "/hdfs/data/to/be/exported",
        "input_null_string": "\\n",
        "input_null_non_string": "\\t",
        "staging_table": "database.staging",
        "clear_staging_table": True,
        "enclosed_by": '"',
        "escaped_by": "\\",
        "input_fields_terminated_by": "|",
        "input_lines_terminated_by": "\n",
        "input_optionally_enclosed_by": '"',
        "batch": True,
        "relaxed_isolation": True,
        "extra_export_options": collections.OrderedDict(
            [("update-key", "id"), ("update-mode", "allowinsert"), ("fetch-size", 1)]
        ),
        "schema": "domino",
    }
    _config_import = {
        "target_dir": "/hdfs/data/target/location",
        "append": True,
        "file_type": "parquet",
        "split_by": "\n",
        "direct": True,
        "driver": "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "extra_import_options": {
            "hcatalog-storage-stanza": '"stored as orcfile"',
            "show": "",
            "fetch-size": 1,
        },
    }

    _config_json = {
        "namenode": "http://0.0.0.0:50070/",
        "job_tracker": "http://0.0.0.0:50030/",
        "libjars": "/path/to/jars",
        "files": "/path/to/files",
        "archives": "/path/to/archives",
    }

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id="sqoop_test",
                conn_type="sqoop",
                schema="schema",
                host="rmdbs",
                port=5050,
                extra=json.dumps(self._config_json),
            )
        )
        db.merge_conn(
            Connection(
                conn_id="sqoop_test_mssql",
                conn_type="mssql",
                schema="schema",
                host="rmdbs",
                port=5050,
                extra=None,
            )
        )

    @patch("subprocess.Popen")
    def test_popen(self, mock_popen):
        # Given
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = StringIO("stdout")
        mock_proc.stderr = StringIO("stderr")
        mock_proc.communicate.return_value = [
            StringIO("stdout\nstdout"),
            StringIO("stderr\nstderr"),
        ]
        mock_popen.return_value.__enter__.return_value = mock_proc

        # When
        hook = SqoopHook(conn_id="sqoop_test")
        hook.export_table(**self._config_export)

        # Then
        assert mock_popen.mock_calls[0] == call(
            [
                "sqoop",
                "export",
                "-fs",
                self._config_json["namenode"],
                "-jt",
                self._config_json["job_tracker"],
                "-libjars",
                self._config_json["libjars"],
                "-files",
                self._config_json["files"],
                "-archives",
                self._config_json["archives"],
                "--connect",
                "rmdbs:5050/schema",
                "--input-null-string",
                self._config_export["input_null_string"],
                "--input-null-non-string",
                self._config_export["input_null_non_string"],
                "--staging-table",
                self._config_export["staging_table"],
                "--clear-staging-table",
                "--enclosed-by",
                self._config_export["enclosed_by"],
                "--escaped-by",
                self._config_export["escaped_by"],
                "--input-fields-terminated-by",
                self._config_export["input_fields_terminated_by"],
                "--input-lines-terminated-by",
                self._config_export["input_lines_terminated_by"],
                "--input-optionally-enclosed-by",
                self._config_export["input_optionally_enclosed_by"],
                "--batch",
                "--relaxed-isolation",
                "--export-dir",
                self._config_export["export_dir"],
                "--update-key",
                "id",
                "--update-mode",
                "allowinsert",
                "--fetch-size",
                str(self._config_export["extra_export_options"].get("fetch-size")),
                "--table",
                self._config_export["table"],
                "--",
                "--schema",
                self._config_export["schema"],
            ],
            stderr=-2,
            stdout=-1,
        )

    def test_submit_none_mappers(self):
        """
        Test to check that if value of num_mappers is None, then it shouldn't be in the cmd built.
        """
        _config_without_mappers = self._config.copy()
        _config_without_mappers["num_mappers"] = None

        hook = SqoopHook(**_config_without_mappers)
        cmd = " ".join(hook._prepare_command())
        assert "--num-mappers" not in cmd

    def test_submit(self):
        """
        Tests to verify that from connection extra option the options are added to the Sqoop command.
        """
        hook = SqoopHook(**self._config)

        cmd = " ".join(hook._prepare_command())

        # Check if the config has been extracted from the json
        if self._config_json["namenode"]:
            assert f"-fs {self._config_json['namenode']}" in cmd

        if self._config_json["job_tracker"]:
            assert f"-jt {self._config_json['job_tracker']}" in cmd

        if self._config_json["libjars"]:
            assert f"-libjars {self._config_json['libjars']}" in cmd

        if self._config_json["files"]:
            assert f"-files {self._config_json['files']}" in cmd

        if self._config_json["archives"]:
            assert f"-archives {self._config_json['archives']}" in cmd

        assert f"--hcatalog-database {self._config['hcatalog_database']}" in cmd
        assert f"--hcatalog-table {self._config['hcatalog_table']}" in cmd

        # Check the regulator stuff passed by the default constructor
        if self._config["verbose"]:
            assert "--verbose" in cmd

        if self._config["num_mappers"]:
            assert f"--num-mappers {self._config['num_mappers']}" in cmd

        for key, value in self._config["properties"].items():
            assert f"-D {key}={value}" in cmd

        # We don't have the sqoop binary available, and this is hard to mock,
        # so just accept an exception for now.
        with pytest.raises(OSError):
            hook.export_table(**self._config_export)

        with pytest.raises(OSError):
            hook.import_table(table="table", target_dir="/sqoop/example/path", schema="schema")

        with pytest.raises(OSError):
            hook.import_query(query="SELECT * FROM sometable", target_dir="/sqoop/example/path")

    def test_export_cmd(self):
        """
        Tests to verify the hook export command is building correct Sqoop export command.
        """
        hook = SqoopHook()

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = " ".join(
            hook._export_cmd(
                self._config_export["table"],
                self._config_export["export_dir"],
                input_null_string=self._config_export["input_null_string"],
                input_null_non_string=self._config_export["input_null_non_string"],
                staging_table=self._config_export["staging_table"],
                clear_staging_table=self._config_export["clear_staging_table"],
                enclosed_by=self._config_export["enclosed_by"],
                escaped_by=self._config_export["escaped_by"],
                input_fields_terminated_by=self._config_export["input_fields_terminated_by"],
                input_lines_terminated_by=self._config_export["input_lines_terminated_by"],
                input_optionally_enclosed_by=self._config_export["input_optionally_enclosed_by"],
                batch=self._config_export["batch"],
                relaxed_isolation=self._config_export["relaxed_isolation"],
                extra_export_options=self._config_export["extra_export_options"],
                schema=self._config_export["schema"],
            )
        )

        assert f"--input-null-string {self._config_export['input_null_string']}" in cmd
        assert f"--input-null-non-string {self._config_export['input_null_non_string']}" in cmd
        assert f"--staging-table {self._config_export['staging_table']}" in cmd
        assert f"--enclosed-by {self._config_export['enclosed_by']}" in cmd
        assert f"--escaped-by {self._config_export['escaped_by']}" in cmd
        assert f"--input-fields-terminated-by {self._config_export['input_fields_terminated_by']}" in cmd
        assert f"--input-lines-terminated-by {self._config_export['input_lines_terminated_by']}" in cmd
        assert f"--input-optionally-enclosed-by {self._config_export['input_optionally_enclosed_by']}" in cmd
        # these options are from the extra export options
        assert "--update-key id" in cmd
        assert "--update-mode allowinsert" in cmd

        if self._config_export["clear_staging_table"]:
            assert "--clear-staging-table" in cmd

        if self._config_export["batch"]:
            assert "--batch" in cmd

        if self._config_export["relaxed_isolation"]:
            assert "--relaxed-isolation" in cmd

        if self._config_export["extra_export_options"]:
            assert "--update-key" in cmd
            assert "--update-mode" in cmd
            assert "--fetch-size" in cmd

        if self._config_export["schema"]:
            assert "-- --schema" in cmd

    def test_import_cmd(self):
        """
        Tests to verify the hook import command is building correct Sqoop import command.
        """
        hook = SqoopHook()

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = " ".join(
            hook._import_cmd(
                self._config_import["target_dir"],
                append=self._config_import["append"],
                file_type=self._config_import["file_type"],
                split_by=self._config_import["split_by"],
                direct=self._config_import["direct"],
                driver=self._config_import["driver"],
                extra_import_options=None,
            )
        )

        if self._config_import["append"]:
            assert "--append" in cmd

        if self._config_import["direct"]:
            assert "--direct" in cmd

        assert f"--target-dir {self._config_import['target_dir']}" in cmd

        assert f"--driver {self._config_import['driver']}" in cmd
        assert f"--split-by {self._config_import['split_by']}" in cmd
        # these are from extra options, but not passed to this cmd import command
        assert "--show" not in cmd
        assert 'hcatalog-storage-stanza "stored as orcfile"' not in cmd

        cmd = " ".join(
            hook._import_cmd(
                target_dir=None,
                append=self._config_import["append"],
                file_type=self._config_import["file_type"],
                split_by=self._config_import["split_by"],
                direct=self._config_import["direct"],
                driver=self._config_import["driver"],
                extra_import_options=self._config_import["extra_import_options"],
            )
        )

        assert "--target-dir" not in cmd
        # these checks are from the extra import options
        assert "--show" in cmd
        assert 'hcatalog-storage-stanza "stored as orcfile"' in cmd
        assert "--fetch-size" in cmd

    def test_get_export_format_argument(self):
        """
        Tests to verify the hook get format function is building
        correct Sqoop command with correct format type.
        """
        hook = SqoopHook()
        assert "--as-avrodatafile" in hook._get_export_format_argument("avro")
        assert "--as-parquetfile" in hook._get_export_format_argument("parquet")
        assert "--as-sequencefile" in hook._get_export_format_argument("sequence")
        assert "--as-textfile" in hook._get_export_format_argument("text")
        with pytest.raises(AirflowException):
            hook._get_export_format_argument("unknown")

    def test_cmd_mask_password(self):
        """
        Tests to verify the hook masking function will correctly mask a user password in Sqoop command.
        """
        hook = SqoopHook()
        assert hook.cmd_mask_password(["--password", "supersecret"]) == ["--password", "MASKED"]

        cmd = ["--target", "targettable"]
        assert hook.cmd_mask_password(cmd) == cmd

    def test_connection_string_preparation(self):
        """
        Tests to verify the hook creates the connection string correctly for mssql and not DB connections.
        """
        # Case mssql
        hook = SqoopHook(conn_id="sqoop_test_mssql")
        assert f"{hook.conn.host}:{hook.conn.port};databaseName={hook.conn.schema}" in hook._prepare_command()

        # Case no mssql
        hook = SqoopHook(conn_id="sqoop_test")
        assert f"{hook.conn.host}:{hook.conn.port}/{hook.conn.schema}" in hook._prepare_command()
