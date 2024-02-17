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
from contextlib import redirect_stdout
from io import StringIO

import pytest
from sqlalchemy import func, select

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import pool_command
from airflow.models import Pool
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_dags, clear_db_pools

pytestmark = pytest.mark.db_test


class TestCliPools:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        clear_db_dags()
        self.dagbag = models.DagBag(include_examples=True)
        self.parser = cli_parser.get_parser()
        self.pools_count_stmt = select([func.count()]).select_from(Pool)
        clear_db_pools()
        with create_session() as session:
            self.session = session
            yield
        clear_db_pools()

    def test_pool_list(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        with redirect_stdout(StringIO()) as stdout:
            pool_command.pool_list(self.parser.parse_args(["pools", "list"]))

        assert "foo" in stdout.getvalue()

    def test_pool_list_with_args(self):
        pool_command.pool_list(self.parser.parse_args(["pools", "list", "--output", "json"]))

    def test_pool_create(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(self.pools_count_stmt) == 2

    def test_pool_update_deferred(self):
        pool_stmt = select(Pool).where(Pool.pool == "foo").limit(1)
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(pool_stmt).include_deferred is False

        pool_command.pool_set(
            self.parser.parse_args(["pools", "set", "foo", "1", "test", "--include-deferred"])
        )
        assert self.session.scalar(pool_stmt).include_deferred is True

        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(pool_stmt).include_deferred is False

    def test_pool_get(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

    def test_pool_delete(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        pool_command.pool_delete(self.parser.parse_args(["pools", "delete", "foo"]))
        assert self.session.scalar(self.pools_count_stmt) == 1

    def test_pool_import_nonexistent(self):
        with pytest.raises(SystemExit):
            pool_command.pool_import(self.parser.parse_args(["pools", "import", "nonexistent.json"]))

    def test_pool_import_invalid_json(self, tmp_path):
        invalid_pool_import_file_path = tmp_path / "pools_import_invalid.json"
        with open(invalid_pool_import_file_path, mode="w") as file:
            file.write("not valid json")

        with pytest.raises(SystemExit):
            pool_command.pool_import(
                self.parser.parse_args(["pools", "import", str(invalid_pool_import_file_path)])
            )

    def test_pool_import_invalid_pools(self, tmp_path):
        invalid_pool_import_file_path = tmp_path / "pools_import_invalid.json"
        pool_config_input = {"foo": {"description": "foo_test", "include_deferred": False}}
        with open(invalid_pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        with pytest.raises(SystemExit):
            pool_command.pool_import(
                self.parser.parse_args(["pools", "import", str(invalid_pool_import_file_path)])
            )

    def test_pool_import_backwards_compatibility(self, tmp_path):
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_config_input = {
            # JSON before version 2.7.0 does not contain `include_deferred`
            "foo": {"description": "foo_test", "slots": 1},
        }
        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

        assert self.session.scalar(select(Pool).where(Pool.pool == "foo").limit(1)).include_deferred is False

    def test_pool_import_export(self, tmp_path):
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_export_file_path = tmp_path / "pools_export.json"
        pool_config_input = {
            "foo": {"description": "foo_test", "slots": 1, "include_deferred": True},
            "default_pool": {"description": "Default pool", "slots": 128, "include_deferred": False},
            "baz": {"description": "baz_test", "slots": 2, "include_deferred": False},
        }
        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        # Import json
        pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

        # Export json
        pool_command.pool_export(self.parser.parse_args(["pools", "export", str(pool_export_file_path)]))

        with open(pool_export_file_path) as file:
            pool_config_output = json.load(file)
            assert pool_config_input == pool_config_output, "Input and output pool files are not same"
