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

import json
import os
import typing
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import asset_command

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, parse_and_sync_to_db

if typing.TYPE_CHECKING:
    from argparse import ArgumentParser

pytestmark = [pytest.mark.db_test]


@pytest.fixture(scope="module", autouse=True)
def prepare_examples():
    parse_and_sync_to_db(os.devnull, include_examples=True)
    yield
    clear_db_runs()
    clear_db_dags()


@pytest.fixture(autouse=True)
def clear_runs():
    clear_db_runs()


@pytest.fixture(scope="module")
def parser() -> ArgumentParser:
    return cli_parser.get_parser()


def test_cli_assets_list(parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "list", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_list(args)

    asset_list = json.loads(capture.getvalue())
    assert len(asset_list) > 0
    assert set(asset_list[0]) == {"name", "uri", "group", "extra"}
    assert any(asset["uri"] == "s3://dag1/output_1.txt" for asset in asset_list), asset_list


def test_cli_assets_alias_list(parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "list", "--alias", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_list(args)

    alias_list = json.loads(capture.getvalue())
    assert len(alias_list) > 0
    assert set(alias_list[0]) == {"name", "group"}
    assert any(alias["name"] == "example-alias" for alias in alias_list), alias_list


def test_cli_assets_details(parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "details", "--name=asset1_producer", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_details(args)

    asset_detail_list = json.loads(capture.getvalue())
    assert len(asset_detail_list) == 1

    # No good way to statically compare these.
    undeterministic = {
        "id": None,
        "created_at": None,
        "updated_at": None,
        "scheduled_dags": None,
        "producing_tasks": None,
        "consuming_tasks": None,
    }

    assert asset_detail_list[0] | undeterministic == undeterministic | {
        "name": "asset1_producer",
        "uri": "s3://bucket/asset1_producer",
        "group": "asset",
        "extra": {},
        "aliases": [],
        "watchers": [],
        "last_asset_event": None,
    }


def test_cli_assets_alias_details(parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "details", "--alias", "--name=example-alias", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_details(args)

    alias_detail_list = json.loads(capture.getvalue())
    assert len(alias_detail_list) == 1

    # No good way to statically compare these.
    undeterministic = {"id": None}

    assert alias_detail_list[0] | undeterministic == undeterministic | {
        "name": "example-alias",
        "group": "asset",
    }


@mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
def test_cli_assets_materialize(mock_hasattr, parser: ArgumentParser, stdout_capture) -> None:
    mock_hasattr.return_value = False
    args = parser.parse_args(["assets", "materialize", "--name=asset1_producer", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_materialize(args)

    output = capture.getvalue()

    # Check if output is empty first
    assert output, "No output captured from asset_materialize command"

    run_list = json.loads(output)
    assert len(run_list) == 1

    # No good way to statically compare these.
    undeterministic: dict = {
        "dag_run_id": None,
        "dag_versions": [],
        "data_interval_end": None,
        "data_interval_start": None,
        "logical_date": None,
        "queued_at": None,
        "run_after": "2025-02-12T19:27:59.066046Z",
    }

    assert run_list[0] | undeterministic == undeterministic | {
        "conf": {},
        "bundle_version": None,
        "dag_display_name": "asset1_producer",
        "dag_id": "asset1_producer",
        "end_date": None,
        "duration": None,
        "last_scheduling_decision": None,
        "note": None,
        "partition_key": None,
        "run_type": "manual",
        "start_date": None,
        "state": "queued",
        "triggered_by": "cli",
        "triggering_user_name": "root",
        "run_after": "2025-02-12T19:27:59.066046Z",
    }


def test_cli_assets_materialize_with_view_url_template(parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "materialize", "--name=asset1_producer", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_materialize(args)

    output = capture.getvalue()
    run_list = json.loads(output)
    assert len(run_list) == 1

    # No good way to statically compare these.
    undeterministic: dict = {
        "dag_run_id": None,
        "dag_versions": [],
        "data_interval_end": None,
        "data_interval_start": None,
        "logical_date": None,
        "queued_at": None,
        "run_after": "2025-02-12T19:27:59.066046Z",
    }

    assert run_list[0] | undeterministic == undeterministic | {
        "conf": {},
        "bundle_version": None,
        "dag_display_name": "asset1_producer",
        "dag_id": "asset1_producer",
        "end_date": None,
        "duration": None,
        "last_scheduling_decision": None,
        "note": None,
        "partition_key": None,
        "run_type": "manual",
        "start_date": None,
        "state": "queued",
        "triggered_by": "cli",
        "triggering_user_name": "root",
        "run_after": "2025-02-12T19:27:59.066046Z",
    }
