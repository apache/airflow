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
from airflow.settings import Session
from airflow.utils.db import add_default_pool_if_not_exists

pytestmark = pytest.mark.db_test


class TestCliPools:
    @classmethod
    def setup_class(cls):
        cls.dagbag = models.DagBag()
        cls.parser = cli_parser.get_parser()
        settings.configure_orm()
        cls.session = Session
        cls._cleanup()

    def tearDown(self):
        self._cleanup()

    @staticmethod
    def _cleanup(session=None):
        if session is None:
            session = Session()
        session.execute(delete(Pool).where(Pool.pool != Pool.DEFAULT_POOL_NAME))
        session.commit()
        add_default_pool_if_not_exists()
        session.close()

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
            "default_pool": {
                "description": "Default pool",
                "slots": 128,
                "include_deferred": False,
            },
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

    def test_pool_set_with_team_name(self):
        """Test that pool_set with --team-name assigns the pool to the team when multi_team is enabled."""
        from airflow.models.team import Team

        from tests_common.test_utils.config import conf_vars

        # Create the team first
        team = Team(name="test_team")
        self.session.add(team)
        self.session.commit()

        try:
            with conf_vars({("core", "multi_team"): "True"}):
                pool_command.pool_set(
                    self.parser.parse_args(
                        ["pools", "set", "team_pool", "5", "team pool", "--team-name", "test_team"]
                    )
                )

            pool = self.session.scalar(select(Pool).where(Pool.pool == "team_pool"))
            assert pool is not None
            assert pool.team_name == "test_team"
            assert pool.slots == 5
        finally:
            self.session.execute(delete(Pool).where(Pool.pool == "team_pool"))
            self.session.execute(delete(Team).where(Team.name == "test_team"))
            self.session.commit()

    def test_pool_set_team_name_rejected_when_multi_team_disabled(self):
        """Test that pool_set with --team-name raises when multi_team is disabled."""
        from airflow.models.team import Team

        from tests_common.test_utils.config import conf_vars

        team = Team(name="test_team")
        self.session.add(team)
        self.session.commit()

        try:
            with conf_vars({("core", "multi_team"): "False"}):
                with pytest.raises(
                    ValueError, match="team_name cannot be set when multi_team mode is disabled"
                ):
                    pool_command.pool_set(
                        self.parser.parse_args(
                            ["pools", "set", "team_pool", "5", "team pool", "--team-name", "test_team"]
                        )
                    )
        finally:
            self.session.execute(delete(Pool).where(Pool.pool == "team_pool"))
            self.session.execute(delete(Team).where(Team.name == "test_team"))
            self.session.commit()

    def test_pool_set_without_team_name(self):
        """Test that pool_set without --team-name leaves team_name as None."""
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "no_team_pool", "3", "no team"]))

        pool = self.session.scalar(select(Pool).where(Pool.pool == "no_team_pool"))
        assert pool is not None
        assert pool.team_name is None

    def test_pool_import_export_with_team_name(self, tmp_path):
        """Test that import/export round-trips the team_name field."""
        from airflow.models.team import Team

        from tests_common.test_utils.config import conf_vars

        team = Team(name="import_team")
        self.session.add(team)
        self.session.commit()

        pool_import_file_path = tmp_path / "pools_import_team.json"
        pool_export_file_path = tmp_path / "pools_export_team.json"
        pool_config_input = {
            "team_pool_a": {
                "slots": 10,
                "description": "team pool",
                "include_deferred": False,
                "team_name": "import_team",
            },
            "global_pool": {
                "slots": 5,
                "description": "global pool",
                "include_deferred": False,
            },
        }

        with open(pool_import_file_path, mode="w") as file:
            json.dump(pool_config_input, file)

        try:
            with conf_vars({("core", "multi_team"): "True"}):
                pool_command.pool_import(
                    self.parser.parse_args(["pools", "import", str(pool_import_file_path)])
                )

            # Verify team assignment
            pool = self.session.scalar(select(Pool).where(Pool.pool == "team_pool_a"))
            assert pool is not None
            assert pool.team_name == "import_team"

            global_pool = self.session.scalar(select(Pool).where(Pool.pool == "global_pool"))
            assert global_pool is not None
            assert global_pool.team_name is None

            # Export and verify
            pool_command.pool_export(self.parser.parse_args(["pools", "export", str(pool_export_file_path)]))

            with open(pool_export_file_path) as file:
                pool_config_output = json.load(file)

            assert pool_config_output["team_pool_a"]["team_name"] == "import_team"
            assert "team_name" not in pool_config_output["global_pool"]
        finally:
            self.session.execute(delete(Pool).where(Pool.pool.in_(["team_pool_a", "global_pool"])))
            self.session.execute(delete(Team).where(Team.name == "import_team"))
            self.session.commit()

    def test_pool_list_shows_team_name(self, stdout_capture):
        """Test that pool list output includes the team_name column."""
        from airflow.models.team import Team

        from tests_common.test_utils.config import conf_vars

        team = Team(name="list_team")
        self.session.add(team)
        self.session.commit()

        try:
            with conf_vars({("core", "multi_team"): "True"}):
                pool_command.pool_set(
                    self.parser.parse_args(
                        ["pools", "set", "list_pool", "5", "desc", "--team-name", "list_team"]
                    )
                )

            with stdout_capture as stdout:
                pool_command.pool_list(self.parser.parse_args(["pools", "list"]))

            output = stdout.getvalue()
            assert "list_team" in output
        finally:
            self.session.execute(delete(Pool).where(Pool.pool == "list_pool"))
            self.session.execute(delete(Team).where(Team.name == "list_team"))
            self.session.commit()
