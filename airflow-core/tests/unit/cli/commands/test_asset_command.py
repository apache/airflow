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
from types import SimpleNamespace

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import asset_command

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, parse_and_sync_to_db

if typing.TYPE_CHECKING:
    from argparse import ArgumentParser

pytestmark = [pytest.mark.db_test]


# Not autouse: only the DB-backed tests below request it, so the mocked (non-DB)
# ``assets materialize`` tests stay free of any database access.
@pytest.fixture(scope="module")
def prepare_examples():
    with conf_vars({("core", "load_examples"): "True"}):
        parse_and_sync_to_db(os.devnull)
    yield
    clear_db_runs()
    clear_db_dags()


@pytest.fixture(scope="module")
def parser() -> ArgumentParser:
    return cli_parser.get_parser()


def test_cli_assets_list(prepare_examples, parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "list", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_list(args)

    asset_list = json.loads(capture.getvalue())
    assert len(asset_list) > 0
    assert set(asset_list[0]) == {"name", "uri", "group", "extra"}
    assert any(asset["uri"] == "s3://dag1/output_1.txt" for asset in asset_list), asset_list


def test_cli_assets_alias_list(prepare_examples, parser: ArgumentParser, stdout_capture) -> None:
    args = parser.parse_args(["assets", "list", "--alias", "--output=json"])
    with stdout_capture as capture:
        asset_command.asset_list(args)

    alias_list = json.loads(capture.getvalue())
    assert len(alias_list) > 0
    assert set(alias_list[0]) == {"name", "group"}
    assert any(alias["name"] == "example-alias" for alias in alias_list), alias_list


def test_cli_assets_details(prepare_examples, parser: ArgumentParser, stdout_capture) -> None:
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


def test_cli_assets_alias_details(prepare_examples, parser: ArgumentParser, stdout_capture) -> None:
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


@pytest.mark.non_db_test_override
class TestCliAssetsMaterialize:
    """`assets materialize` goes through the airflowctl client; mocked here (no DB/server)."""

    def test_materialize(self, parser: ArgumentParser, mock_cli_api_client, stdout_capture) -> None:
        mock_cli_api_client.assets.list.return_value.assets = [
            SimpleNamespace(id=7, name="asset1_producer", uri="s3://bucket/asset1_producer"),
            SimpleNamespace(id=8, name="other", uri="s3://bucket/other"),
        ]
        mock_cli_api_client.assets.materialize.return_value.model_dump.return_value = {
            "dag_id": "asset1_producer",
            "run_type": "asset_materialization",
            "state": "queued",
        }
        args = parser.parse_args(["assets", "materialize", "--name=asset1_producer", "--output=json"])
        with stdout_capture as capture:
            asset_command.asset_materialize(args)

        run_list = json.loads(capture.getvalue())
        assert len(run_list) == 1
        assert run_list[0]["dag_id"] == "asset1_producer"
        # The asset is resolved to its id and materialization is delegated to the API server.
        mock_cli_api_client.assets.materialize.assert_called_once_with(asset_id="7")

    def test_materialize_requires_name_or_uri(self, parser: ArgumentParser, mock_cli_api_client) -> None:
        with pytest.raises(SystemExit, match="Either --name or --uri is required"):
            asset_command.asset_materialize(parser.parse_args(["assets", "materialize"]))
        mock_cli_api_client.assets.materialize.assert_not_called()

    def test_materialize_missing(self, parser: ArgumentParser, mock_cli_api_client) -> None:
        mock_cli_api_client.assets.list.return_value.assets = []
        with pytest.raises(SystemExit, match="Asset with name nope does not exist"):
            asset_command.asset_materialize(parser.parse_args(["assets", "materialize", "--name=nope"]))
        mock_cli_api_client.assets.materialize.assert_not_called()

    def test_materialize_ambiguous(self, parser: ArgumentParser, mock_cli_api_client) -> None:
        mock_cli_api_client.assets.list.return_value.assets = [
            SimpleNamespace(id=1, name="dup", uri="s3://a"),
            SimpleNamespace(id=2, name="dup", uri="s3://b"),
        ]
        with pytest.raises(SystemExit, match="More than one asset exists with name dup"):
            asset_command.asset_materialize(parser.parse_args(["assets", "materialize", "--name=dup"]))
        mock_cli_api_client.assets.materialize.assert_not_called()
