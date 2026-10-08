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
import json
import stat
from unittest import mock

import jwt
import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_processor_token_command
from airflow.dag_processing.bundles.manager import _load_bundle_config_snapshot

from tests_common.test_utils.config import conf_vars

# Classes that are not installed: provisioning must not need the bundle implementations.
BUNDLE_CONFIG = [
    {"name": "bundle_a", "classpath": "not_installed.bundles.BundleA", "kwargs": {}},
    {"name": "bundle_b", "classpath": "not_installed.bundles.BundleB", "kwargs": {}},
]


class _StopRotation(Exception):
    pass


def _decode(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


@pytest.fixture(autouse=True)
def provisioning_config():
    with conf_vars(
        {
            ("api_auth", "jwt_secret"): "provisioning-test-secret",
            ("execution_api", "jwt_expiration_time"): "600",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(BUNDLE_CONFIG),
            ("core", "load_examples"): "False",
        }
    ):
        _load_bundle_config_snapshot.cache_clear()
        yield
    _load_bundle_config_snapshot.cache_clear()


class TestDagProcessorTokenCommand:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def _run(self, *args: str) -> None:
        dag_processor_token_command.dag_processor_token(
            self.parser.parse_args(["dag-processor-token", *args])
        )

    def test_grants_every_configured_bundle_by_default(self, tmp_path):
        token_file = tmp_path / "token"

        self._run("--token-file", str(token_file))

        claims = _decode(token_file.read_text())
        assert (claims["scope"], claims["dag_bundles"]) == ("dag_processor_session", ["bundle_a", "bundle_b"])
        assert claims["exp"] - claims["iat"] == 600

    def test_grants_the_requested_bundles(self, tmp_path):
        token_file = tmp_path / "token"

        self._run("--token-file", str(token_file), "-B", "bundle_b", "--valid-for", "120")

        claims = _decode(token_file.read_text())
        assert claims["dag_bundles"] == ["bundle_b"]
        assert claims["exp"] - claims["iat"] == 120

    def test_rejects_an_unknown_bundle(self, tmp_path):
        token_file = tmp_path / "token"

        with pytest.raises(SystemExit, match="Bundles not found: unknown"):
            self._run("--token-file", str(token_file), "-B", "unknown")

        assert not token_file.exists()

    @mock.patch.object(dag_processor_token_command, "write_token_file", autospec=True)
    @mock.patch.object(
        dag_processor_token_command.time, "sleep", autospec=True, side_effect=[None, _StopRotation]
    )
    def test_rotation_keeps_the_session(self, mock_sleep, mock_write, tmp_path):
        with pytest.raises(_StopRotation):
            self._run("--token-file", str(tmp_path / "token"), "--valid-for", "120", "--rotate")

        first, second = (_decode(call.args[1]) for call in mock_write.call_args_list)
        assert first["sub"] == second["sub"]
        assert first["jti"] != second["jti"]
        mock_sleep.assert_called_with(60)


class TestWriteTokenFile:
    def test_replaces_the_file_readable_only_by_its_owner(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")

        dag_processor_token_command.write_token_file(token_file, "new")

        assert token_file.read_text() == "new"
        assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
        assert [path.name for path in tmp_path.iterdir()] == ["token"]

    @mock.patch(
        "airflow.cli.commands.dag_processor_token_command.os.replace",
        autospec=True,
        side_effect=OSError("disk full"),
    )
    def test_failure_keeps_the_previous_token(self, _, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")

        with pytest.raises(OSError, match="disk full"):
            dag_processor_token_command.write_token_file(token_file, "new")

        assert token_file.read_text() == "old"
        assert [path.name for path in tmp_path.iterdir()] == ["token"]
