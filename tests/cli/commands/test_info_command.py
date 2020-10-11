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

import contextlib
import importlib
import io
import os
import unittest
from unittest import mock

from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import info_command
from airflow.config_templates import airflow_local_settings
from airflow.logging_config import configure_logging
from airflow.version import version as airflow_version
from tests.test_utils.config import conf_vars


class TestPiiAnonymizer(unittest.TestCase):
    def setUp(self) -> None:
        self.instance = info_command.PiiAnonymizer()

    def test_should_remove_pii_from_path(self):
        home_path = os.path.expanduser("~/airflow/config")
        self.assertEqual("${HOME}/airflow/config", self.instance.process_path(home_path))

    @parameterized.expand(
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://p...s@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            ("postgresql+psycopg2://postgres/airflow", "postgresql+psycopg2://postgres/airflow",),
        ]
    )
    def test_should_remove_pii_from_url(self, before, after):
        self.assertEqual(after, self.instance.process_url(before))


class TestAirflowInfo(unittest.TestCase):
    def test_should_be_string(self):
        text = str(info_command.AirflowInfo(info_command.NullAnonymizer()))

        self.assertIn(f"Apache Airflow [{airflow_version}]", text)


class TestSystemInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(info_command.SystemInfo(info_command.NullAnonymizer())))


class TestPathsInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(info_command.PathsInfo(info_command.NullAnonymizer())))


class TestConfigInfo(unittest.TestCase):
    @conf_vars(
        {
            ("core", "executor"): "TEST_EXECUTOR",
            ("core", "dags_folder"): "TEST_DAGS_FOLDER",
            ("core", "plugins_folder"): "TEST_PLUGINS_FOLDER",
            ("logging", "base_log_folder"): "TEST_LOG_FOLDER",
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_should_read_config(self):
        instance = info_command.ConfigInfo(info_command.NullAnonymizer())
        text = str(instance)
        self.assertIn("TEST_EXECUTOR", text)
        self.assertIn("TEST_DAGS_FOLDER", text)
        self.assertIn("TEST_PLUGINS_FOLDER", text)
        self.assertIn("TEST_LOG_FOLDER", text)
        self.assertIn("postgresql+psycopg2://postgres:airflow@postgres/airflow", text)


class TestConfigInfoLogging(unittest.TestCase):
    def test_should_read_logging_configuration(self):
        with conf_vars({
            ('logging', 'remote_logging'): 'True',
            ('logging', 'remote_base_log_folder'): 'stackdriver://logs-name',
        }):
            importlib.reload(airflow_local_settings)
            configure_logging()
            instance = info_command.ConfigInfo(info_command.NullAnonymizer())
            text = str(instance)
            self.assertIn("StackdriverTaskHandler", text)

    def tearDown(self) -> None:
        importlib.reload(airflow_local_settings)
        configure_logging()


class TestToolsInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(info_command.ToolsInfo(info_command.NullAnonymizer())))


class TestShowInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info"]))

        output = stdout.getvalue()
        self.assertIn(f"Apache Airflow [{airflow_version}]", output)
        self.assertIn("postgresql+psycopg2://postgres:airflow@postgres/airflow", output)

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info_anonymize(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info", "--anonymize"]))

        output = stdout.getvalue()
        self.assertIn(f"Apache Airflow [{airflow_version}]", output)
        self.assertIn("postgresql+psycopg2://p...s:PASSWORD@postgres/airflow", output)

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    @mock.patch(
        "airflow.cli.commands.info_command.requests",
        **{  # type: ignore
            "post.return_value.ok": True,
            "post.return_value.json.return_value": {
                "success": True,
                "key": "f9U3zs3I",
                "link": "https://file.io/TEST",
                "expiry": "14 days",
            },
        }
    )
    def test_show_info_anonymize_fileio(self, mock_requests):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info", "--file-io"]))

        self.assertIn("https://file.io/TEST", stdout.getvalue())
        content = mock_requests.post.call_args[1]["files"]["file"][1]
        self.assertIn("postgresql+psycopg2://p...s:PASSWORD@postgres/airflow", content)
