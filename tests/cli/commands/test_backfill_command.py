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
from datetime import datetime
from unittest import mock

import pendulum
import pytest

import airflow.cli.commands.backfill_command
from airflow.cli import cli_parser
from airflow.models import DagBag
from airflow.models.backfill import ReprocessBehavior
from airflow.utils import timezone

from tests_common.test_utils.db import clear_db_backfills, clear_db_dags, clear_db_runs

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)
if pendulum.__version__.startswith("3"):
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat(sep=" ")
else:
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat()

# TODO: Check if tests needs side effects - locally there's missing DAG

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestCliBackfill:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.dagbag.sync_to_db()
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
            "--dag",
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
        )
