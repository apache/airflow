import contextlib
import io
import os
import unittest
from unittest import mock

from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import info_command
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

        self.assertIn("Apache Airflow [{}]".format(airflow_version), text)


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
            ("core", "SQL_ALCHEMY_CONN"): "postgresql+psycopg2://postgres:airflow@postgres/airflow",
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


class TestToolsInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(info_command.ToolsInfo(info_command.NullAnonymizer())))


class TestShowInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def test_show_info(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info"]))

        self.assertIn("Apache Airflow [{}]".format(airflow_version), stdout.getvalue())

    def test_show_info_anonymize(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info", "--anonymize"]))

        self.assertIn("Apache Airflow [{}]".format(airflow_version), stdout.getvalue())

    @mock.patch(
        "airflow.cli.commands.info_command.requests",
        **{
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
            info_command.show_info(self.parser.parse_args(["info", "--anonymize", "--file-io"]))

        self.assertIn("https://file.io/TEST", stdout.getvalue())
