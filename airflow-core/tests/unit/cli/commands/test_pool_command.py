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
from types import SimpleNamespace

import httpx
import pytest
from airflowctl.api.operations import ServerResponseError

from airflow.cli import cli_parser
from airflow.cli.commands import pool_command

from tests_common.test_utils.config import conf_vars


def _pool(name, slots, description="", include_deferred=False, team_name=None):
    """Build a stand-in for the airflowctl ``PoolResponse`` returned by the client."""
    return SimpleNamespace(
        name=name,
        slots=slots,
        description=description,
        include_deferred=include_deferred,
        team_name=team_name,
    )


def _server_error(status_code: int) -> ServerResponseError:
    request = httpx.Request("GET", "http://testserver/api/v2/pools/foo")
    response = httpx.Response(status_code, request=request, json={"detail": "boom"})
    return ServerResponseError(message="boom", request=request, response=response)


class TestCliPools:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_pool_list(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.pools.list.return_value.pools = [_pool("foo", 1, "test")]
        with stdout_capture as stdout:
            pool_command.pool_list(self.parser.parse_args(["pools", "list"]))

        assert "foo" in stdout.getvalue()
        mock_cli_api_client.pools.list.assert_called_once()

    def test_pool_list_with_args(self, mock_cli_api_client):
        mock_cli_api_client.pools.list.return_value.pools = [_pool("foo", 1, "test")]
        pool_command.pool_list(self.parser.parse_args(["pools", "list", "--output", "json"]))

    def test_pool_create(self, mock_cli_api_client):
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "foo", "1", "test"]))

        mock_cli_api_client.pools.create.assert_called_once()
        body = mock_cli_api_client.pools.create.call_args.kwargs["pool"]
        # core_api PoolBody exposes the name via the ``pool`` attribute (alias ``name``).
        assert body.pool == "foo"
        assert body.slots == 1
        assert body.description == "test"
        assert body.include_deferred is False

    def test_pool_create_include_deferred(self, mock_cli_api_client):
        pool_command.pool_set(
            self.parser.parse_args(["pools", "set", "foo", "1", "test", "--include-deferred"])
        )

        body = mock_cli_api_client.pools.create.call_args.kwargs["pool"]
        assert body.include_deferred is True

    def test_pool_get(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.pools.get.return_value = _pool("foo", 1, "test")
        with stdout_capture as stdout:
            pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

        assert "foo" in stdout.getvalue()
        mock_cli_api_client.pools.get.assert_called_once_with(pool_name="foo")

    def test_pool_get_missing(self, mock_cli_api_client):
        mock_cli_api_client.pools.get.side_effect = _server_error(404)
        with pytest.raises(SystemExit, match="Pool foo does not exist"):
            pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

    def test_pool_get_other_error_reraised(self, mock_cli_api_client):
        mock_cli_api_client.pools.get.side_effect = _server_error(500)
        with pytest.raises(ServerResponseError):
            pool_command.pool_get(self.parser.parse_args(["pools", "get", "foo"]))

    def test_pool_delete(self, mock_cli_api_client):
        pool_command.pool_delete(self.parser.parse_args(["pools", "delete", "foo"]))
        mock_cli_api_client.pools.delete.assert_called_once_with(pool="foo")

    def test_pool_delete_missing(self, mock_cli_api_client):
        mock_cli_api_client.pools.delete.side_effect = _server_error(404)
        with pytest.raises(SystemExit, match="Pool foo does not exist"):
            pool_command.pool_delete(self.parser.parse_args(["pools", "delete", "foo"]))

    def test_pool_import_nonexistent(self, mock_cli_api_client):
        with pytest.raises(SystemExit):
            pool_command.pool_import(self.parser.parse_args(["pools", "import", "nonexistent.json"]))

    def test_pool_import_invalid_json(self, mock_cli_api_client, tmp_path):
        invalid_pool_import_file_path = tmp_path / "pools_import_invalid.json"
        invalid_pool_import_file_path.write_text("not valid json")

        with pytest.raises(SystemExit):
            pool_command.pool_import(
                self.parser.parse_args(["pools", "import", str(invalid_pool_import_file_path)])
            )

    def test_pool_import_invalid_pools(self, mock_cli_api_client, tmp_path):
        invalid_pool_import_file_path = tmp_path / "pools_import_invalid.json"
        # Missing ``slots`` makes the entry invalid.
        pool_config_input = {"foo": {"description": "foo_test", "include_deferred": False}}
        invalid_pool_import_file_path.write_text(json.dumps(pool_config_input))

        with pytest.raises(SystemExit):
            pool_command.pool_import(
                self.parser.parse_args(["pools", "import", str(invalid_pool_import_file_path)])
            )

    def test_pool_import(self, mock_cli_api_client, tmp_path):
        pool_import_file_path = tmp_path / "pools_import.json"
        pool_config_input = {
            "foo": {"description": "foo_test", "slots": 1, "include_deferred": True},
            # JSON before version 2.7.0 does not contain ``include_deferred``.
            "bar": {"description": "bar_test", "slots": 2},
        }
        pool_import_file_path.write_text(json.dumps(pool_config_input))

        pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

        assert mock_cli_api_client.pools.create.call_count == 2
        bodies = {
            call.kwargs["pool"].pool: call.kwargs["pool"]
            for call in mock_cli_api_client.pools.create.call_args_list
        }
        assert bodies["foo"].include_deferred is True
        # Missing ``include_deferred`` defaults to False (backwards compatibility).
        assert bodies["bar"].include_deferred is False

    def test_pool_export(self, mock_cli_api_client, tmp_path):
        pool_export_file_path = tmp_path / "pools_export.json"
        mock_cli_api_client.pools.list.return_value.pools = [
            _pool("foo", 1, "foo_test", include_deferred=True),
            _pool("baz", 2, "baz_test", include_deferred=False),
        ]

        pool_command.pool_export(self.parser.parse_args(["pools", "export", str(pool_export_file_path)]))

        exported = json.loads(pool_export_file_path.read_text())
        assert exported == {
            "foo": {"slots": 1, "description": "foo_test", "include_deferred": True},
            "baz": {"slots": 2, "description": "baz_test", "include_deferred": False},
        }

    def test_pool_set_with_team_name(self, mock_cli_api_client):
        """``--team-name`` is forwarded to the airflowctl client when multi_team is enabled."""
        with conf_vars({("core", "multi_team"): "True"}):
            pool_command.pool_set(
                self.parser.parse_args(
                    ["pools", "set", "team_pool", "5", "team pool", "--team-name", "test_team"]
                )
            )

        body = mock_cli_api_client.pools.create.call_args.kwargs["pool"]
        assert body.team_name == "test_team"

    def test_pool_set_team_name_rejected_when_multi_team_disabled(self, mock_cli_api_client):
        """``PoolBody`` rejects a team_name (client-side) when multi_team is disabled."""
        with conf_vars({("core", "multi_team"): "False"}):
            with pytest.raises(ValueError, match="team_name cannot be set when multi_team mode is disabled"):
                pool_command.pool_set(
                    self.parser.parse_args(
                        ["pools", "set", "team_pool", "5", "team pool", "--team-name", "test_team"]
                    )
                )
        mock_cli_api_client.pools.create.assert_not_called()

    def test_pool_set_without_team_name(self, mock_cli_api_client):
        """Without ``--team-name`` the forwarded body has ``team_name`` as None."""
        pool_command.pool_set(self.parser.parse_args(["pools", "set", "no_team_pool", "3", "no team"]))

        body = mock_cli_api_client.pools.create.call_args.kwargs["pool"]
        assert body.team_name is None

    def test_pool_import_forwards_team_name(self, mock_cli_api_client, tmp_path):
        """Import forwards each pool's ``team_name`` (or None) to the airflowctl client."""
        pool_import_file_path = tmp_path / "pools_import_team.json"
        pool_import_file_path.write_text(
            json.dumps(
                {
                    "team_pool_a": {
                        "slots": 10,
                        "description": "team pool",
                        "include_deferred": False,
                        "team_name": "import_team",
                    },
                    "global_pool": {"slots": 5, "description": "global pool", "include_deferred": False},
                }
            )
        )

        with conf_vars({("core", "multi_team"): "True"}):
            pool_command.pool_import(self.parser.parse_args(["pools", "import", str(pool_import_file_path)]))

        bodies = {
            call.kwargs["pool"].pool: call.kwargs["pool"]
            for call in mock_cli_api_client.pools.create.call_args_list
        }
        assert bodies["team_pool_a"].team_name == "import_team"
        assert bodies["global_pool"].team_name is None

    def test_pool_export_includes_team_name(self, mock_cli_api_client, tmp_path):
        """Export writes ``team_name`` only for pools that have one."""
        pool_export_file_path = tmp_path / "pools_export_team.json"
        mock_cli_api_client.pools.list.return_value.pools = [
            _pool("team_pool_a", 10, "team pool", team_name="import_team"),
            _pool("global_pool", 5, "global pool"),
        ]

        pool_command.pool_export(self.parser.parse_args(["pools", "export", str(pool_export_file_path)]))

        exported = json.loads(pool_export_file_path.read_text())
        assert exported["team_pool_a"]["team_name"] == "import_team"
        assert "team_name" not in exported["global_pool"]

    def test_pool_list_shows_team_name(self, mock_cli_api_client, stdout_capture):
        """Pool list output includes the team_name column."""
        mock_cli_api_client.pools.list.return_value.pools = [
            _pool("list_pool", 5, "desc", team_name="list_team")
        ]

        with stdout_capture as stdout:
            pool_command.pool_list(self.parser.parse_args(["pools", "list"]))

        assert "list_team" in stdout.getvalue()
