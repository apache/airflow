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

import pytest
from sqlalchemy import delete, func, select

from airflow import models, settings
from airflow.cli import cli_parser
from airflow.cli.commands import pool_command
from airflow.models import Pool
from airflow.models.team import Team
from airflow.settings import Session
from airflow.utils.db import add_default_pool_if_not_exists
from airflow.utils.session import provide_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_teams

pytestmark = pytest.mark.db_test


@provide_session
def _create_teams(session) -> None:
    session.add(Team(name="test_team"))
    session.add(Team(name="team_one"))
    session.add(Team(name="team_two"))
    session.commit()


class TestCliPools:
    @classmethod
    def setup_class(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()
        settings.configure_orm()
        cls.session = Session
        cls._cleanup()

    def teardown_method(self):
        self._cleanup()

    @staticmethod
    def _cleanup(session=None):
        if session is None:
            session = Session()
        clear_db_teams()
        session.execute(delete(Pool).where(Pool.pool != Pool.DEFAULT_POOL_NAME))
        session.commit()
        add_default_pool_if_not_exists()
        session.close()

    def create_teams(self):
        _create_teams(self.session)

    def test_pool_list(self, stdout_capture):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        with stdout_capture as stdout:
            pool_command.pool_list(self.parser.parse_args(["pools", "list"]))

        assert "foo" in stdout.getvalue()

    def test_pool_list_with_args(self):
        pool_command.pool_list(self.parser.parse_args(["pools", "list", "--output", "json"]))

    def test_pool_create(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(select(func.count()).select_from(Pool)) == 2

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_create_with_team(self):
        self.create_teams()
        pool_command.pool_set(
            self.parser.parse_args(["pools", "set", "foo", "1", "test", "--team-name", "test_team"])
        )
        assert self.session.scalar(select(func.count()).select_from(Pool)) == 2

    @conf_vars({("core", "multi_team"): "False"})
    def test_pool_create_rejects_team_name_when_multi_team_disabled(self):
        with pytest.raises(
            SystemExit,
            match="Error: team_name cannot be set when multi_team mode is disabled. Please contact your administrator.",
        ):
            pool_command.pool_set(
                self.parser.parse_args(["pools", "set", "foo", "1", "test", "--team-name", "test_team"])
            )

    def test_pool_update_deferred(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(select(Pool).where(Pool.pool == "foo")).include_deferred is False

        pool_command.pool_set(
            self.parser.parse_args(["pools", "set", "foo", "1", "test", "--include-deferred"])
        )
        assert self.session.scalar(select(Pool).where(Pool.pool == "foo")).include_deferred is True

        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        assert self.session.scalar(select(Pool).where(Pool.pool == "foo")).include_deferred is False

    def test_pool_get(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_get_with_team(self):
        self.create_teams()
        pool_command.pool_set(
            self.parser.parse_args(["pools", "set", "foo", "1", "test", "--team-name", "test_team"])
        )
        pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

    def test_pool_delete(self):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))
        pool_command.pool_delete(self.parser.parse_args(["pools", "delete", "foo"]))
        assert self.session.scalar(select(func.count()).select_from(Pool)) == 1

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

    @conf_vars({("core", "multi_team"): "False"})
    def test_pool_import_rejects_team_name_when_multi_team_disabled(self, tmp_path):
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_config_input = {
            "team_one_pool": {
                "description": "team one pool",
                "slots": 1,
                "include_deferred": True,
                "team_name": "team_one",
            },
            "default_pool": {
                "description": "Default pool",
                "slots": 128,
                "include_deferred": False,
                "team_name": None,
            },
            "team_two_pool": {
                "description": "team two pool",
                "slots": 2,
                "include_deferred": False,
                "team_name": "team_two",
            },
        }
        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        # Import json
        with pytest.raises(
            SystemExit,
            match="Error: team_name cannot be set when multi_team mode is disabled. Please contact your administrator.",
        ):
            pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_import_with_fallback_team_name(self, tmp_path):
        self.create_teams()
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_config_input = {
            "team_one_pool": {"description": "team one pool", "slots": 1, "include_deferred": True},
            "default_pool": {"description": "Default pool", "slots": 128, "include_deferred": False},
            "team_two_pool": {
                "description": "team two pool",
                "slots": 2,
                "include_deferred": False,
                "team_name": "team_two",
            },
        }
        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        # Import json
        pool_command.pool_import(
            self.parser.parse_args(["pools", "import", str(pool_import_file_path), "--team-name", "team_one"])
        )

        assert self.session.scalar(select(Pool).where(Pool.pool == "team_one_pool")).team_name == "team_one"
        assert self.session.scalar(select(Pool).where(Pool.pool == "default_pool")).team_name == "team_one"
        assert self.session.scalar(select(Pool).where(Pool.pool == "team_two_pool")).team_name == "team_two"

    def test_pool_import_backwards_compatibility(self, tmp_path):
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_config_input = {
            # JSON before version 2.7.0 does not contain `include_deferred`
            "foo": {"description": "foo_test", "slots": 1},
        }
        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

        assert self.session.scalar(select(Pool).where(Pool.pool == "foo")).include_deferred is False

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

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_import_export_with_team(self, tmp_path):
        self.create_teams()
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_export_file_path = tmp_path / "pools_export.json"
        pool_config_input = {
            "team_one_pool": {
                "description": "team one pool",
                "slots": 1,
                "include_deferred": True,
                "team_name": "team_one",
            },
            "default_pool": {
                "description": "Default pool",
                "slots": 128,
                "include_deferred": False,
                "team_name": None,
            },
            "team_two_pool": {
                "description": "team two pool",
                "slots": 2,
                "include_deferred": False,
                "team_name": "team_two",
            },
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
