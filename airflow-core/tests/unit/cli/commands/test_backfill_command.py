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

import argparse
import os
from datetime import datetime
from unittest import mock

import pendulum
import pytest

import airflow.cli.commands.backfill_command
from airflow._shared.timezones import timezone
from airflow.cli import cli_parser
from airflow.models.backfill import ReprocessBehavior

from tests_common.test_utils.db import clear_db_backfills, clear_db_dags, clear_db_runs, parse_and_sync_to_db

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)
if pendulum.__version__.startswith("3"):
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat(sep=" ")
else:
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat()

# TODO: Check if tests needs side effects - locally there's missing DAG

pytestmark = pytest.mark.db_test


class TestCliBackfill:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        parse_and_sync_to_db(os.devnull, include_examples=True)
        cls.parser = cli_parser.get_parser()

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_backfills()

    def setup_method(self):
        clear_db_runs()  # clean-up all dag run before start each test
        clear_db_dags()
        clear_db_backfills()

    @mock.patch("airflow.cli.commands.backfill_command._create_backfill")
    @pytest.mark.parametrize(
        "repro, expected_repro",
        [
            (None, None),
            ("none", ReprocessBehavior.NONE),
            ("completed", ReprocessBehavior.COMPLETED),
            ("failed", ReprocessBehavior.FAILED),
        ],
    )
    def test_backfill(self, mock_create, repro, expected_repro):
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
        ]
        if repro is not None:
            args.extend(
                [
                    "--reprocess-behavior",
                    repro,
                ]
            )
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_create.assert_called_once_with(
            dag_id="example_bash_operator",
            from_date=DEFAULT_DATE,
            to_date=DEFAULT_DATE,
            max_active_runs=None,
            reverse=False,
            dag_run_conf=None,
            reprocess_behavior=expected_repro,
            triggering_user_name="root",
            run_on_latest_version=True,
        )

    @mock.patch("airflow.cli.commands.backfill_command._create_backfill")
    def test_backfill_with_run_on_latest_version(self, mock_create):
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
            "--run-on-latest-version",
        ]
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_create.assert_called_once_with(
            dag_id="example_bash_operator",
            from_date=DEFAULT_DATE,
            to_date=DEFAULT_DATE,
            max_active_runs=None,
            reverse=False,
            dag_run_conf=None,
            reprocess_behavior=None,
            run_on_latest_version=True,
            triggering_user_name="root",
        )

    @mock.patch("airflow.cli.commands.backfill_command._do_dry_run")
    @pytest.mark.parametrize(
        "reverse",
        [False, True],
    )
    def test_backfill_dry_run(self, mock_dry_run, reverse):
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
            "--dry-run",
            "--reprocess-behavior",
            "none",
        ]
        if reverse:
            args.append("--run-backwards")
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_dry_run.assert_called_once_with(
            dag_id="example_bash_operator",
            from_date=DEFAULT_DATE.replace(tzinfo=timezone.utc),
            to_date=DEFAULT_DATE.replace(tzinfo=timezone.utc),
            reverse=reverse,
            reprocess_behavior="none",
            session=mock.ANY,
        )

    @mock.patch("airflow.cli.commands.backfill_command._create_backfill")
    def test_backfill_with_dag_run_conf(self, mock_create):
        """Test that dag_run_conf is properly parsed from JSON string."""
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
            "--dag-run-conf",
            '{"example_key": "example_value"}',
        ]
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_create.assert_called_once_with(
            dag_id="example_bash_operator",
            from_date=DEFAULT_DATE,
            to_date=DEFAULT_DATE,
            max_active_runs=None,
            reverse=False,
            dag_run_conf={"example_key": "example_value"},
            reprocess_behavior=None,
            triggering_user_name="root",
            run_on_latest_version=True,
        )

    def test_backfill_with_invalid_dag_run_conf(self):
        """Test that invalid JSON in dag_run_conf raises ValueError."""
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
            "--dag-run-conf",
            '{"invalid": json}',  # Invalid JSON
        ]
        with pytest.raises(ValueError, match="Invalid JSON in --dag-run-conf"):
            airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

    @mock.patch("airflow.cli.commands.backfill_command._create_backfill")
    def test_backfill_with_empty_dag_run_conf(self, mock_create):
        """Test that empty dag_run_conf is properly parsed."""
        args = [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
            "--dag-run-conf",
            "{}",
        ]
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_create.assert_called_once_with(
            dag_id="example_bash_operator",
            from_date=DEFAULT_DATE,
            to_date=DEFAULT_DATE,
            max_active_runs=None,
            reverse=False,
            dag_run_conf={},
            reprocess_behavior=None,
            triggering_user_name="root",
            run_on_latest_version=True,
        )
