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
from unittest import mock

import jwt
import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_processor_token_command

from tests_common.test_utils.config import conf_vars

CONFIGURED_BUNDLES = ["bundle_a", "bundle_b"]


class _StopRotation(Exception):
    pass


def _decode(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


@pytest.fixture(autouse=True)
def signing_config():
    with conf_vars(
        {
            ("api_auth", "jwt_secret"): "provisioning-test-secret",
            ("execution_api", "jwt_expiration_time"): "600",
        }
    ):
        yield


@mock.patch.object(dag_processor_token_command, "DagBundlesManager", autospec=True)
class TestDagProcessorTokenCommand:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def _run(self, mock_manager, *args: str) -> None:
        mock_manager.return_value.get_all_bundle_names.return_value = CONFIGURED_BUNDLES
        dag_processor_token_command.dag_processor_token(
            self.parser.parse_args(["dag-processor-token", *args])
        )

    def test_grants_every_configured_bundle_by_default(self, mock_manager, tmp_path):
        token_file = tmp_path / "token"

        self._run(mock_manager, "--token-file", str(token_file))

        claims = _decode(token_file.read_text())
        assert (claims["scope"], claims["dag_bundles"]) == ("dag_processor", CONFIGURED_BUNDLES)
        assert claims["exp"] - claims["iat"] == 600

    def test_grants_the_requested_bundles(self, mock_manager, tmp_path):
        token_file = tmp_path / "token"

        self._run(mock_manager, "--token-file", str(token_file), "-B", "bundle_b", "--valid-for", "120")

        claims = _decode(token_file.read_text())
        assert claims["dag_bundles"] == ["bundle_b"]
        assert claims["exp"] - claims["iat"] == 120

    def test_rejects_an_unknown_bundle(self, mock_manager, tmp_path):
        token_file = tmp_path / "token"

        with pytest.raises(SystemExit, match="Bundles not found: unknown"):
            self._run(mock_manager, "--token-file", str(token_file), "-B", "unknown")

        assert not token_file.exists()

    @mock.patch.object(dag_processor_token_command, "write_token_file", autospec=True)
    @mock.patch.object(
        dag_processor_token_command.time, "sleep", autospec=True, side_effect=[None, _StopRotation]
    )
    def test_rotation_keeps_the_session(self, mock_sleep, mock_write, mock_manager, tmp_path):
        with pytest.raises(_StopRotation):
            self._run(mock_manager, "--token-file", str(tmp_path / "token"), "--valid-for", "120", "--rotate")

        first, second = (_decode(call.args[1]) for call in mock_write.call_args_list)
        assert first["sub"] == second["sub"]
        assert first["jti"] != second["jti"]
        mock_sleep.assert_called_with(60)
