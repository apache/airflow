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

import contextlib
import io
import json
import typing

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import asset_command
from airflow.models.dagbag import DagBag

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags

if typing.TYPE_CHECKING:
    from argparse import ArgumentParser

pytestmark = [pytest.mark.db_test]


@pytest.fixture(scope="module", autouse=True)
def prepare_examples():
    DagBag(include_examples=True).sync_to_db()
    yield
    clear_db_dags()


@pytest.fixture(scope="module")
def parser() -> ArgumentParser:
    return cli_parser.get_parser()


@conf_vars({("core", "load_examples"): "true"})
def test_cli_assets_list(parser: ArgumentParser) -> None:
    args = parser.parse_args(["assets", "list", "--output=json"])
    with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
        asset_command.asset_list(args)

    asset_list = json.loads(temp_stdout.getvalue())
    assert len(asset_list) > 0
    assert "name" in asset_list[0]
    assert "uri" in asset_list[0]
    assert "group" in asset_list[0]
    assert "extra" in asset_list[0]
    assert any(asset["uri"] == "s3://dag1/output_1.txt" for asset in asset_list)
