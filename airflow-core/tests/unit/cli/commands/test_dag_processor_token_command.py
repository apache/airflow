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
import os
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

    def test_restart_keeps_the_session_for_the_same_bundles(self, tmp_path):
        token_file = tmp_path / "token"
        self._run("--token-file", str(token_file))
        first = _decode(token_file.read_text())

        self._run("--token-file", str(token_file))

        assert _decode(token_file.read_text())["sub"] == first["sub"]

    @pytest.mark.parametrize(
        "previous",
        [
            pytest.param(None, id="other-bundles"),
            pytest.param("not a token", id="unreadable"),
        ],
    )
    def test_restart_starts_a_new_session_when_the_file_cannot_be_reused(self, tmp_path, previous):
        token_file = tmp_path / "token"
        self._run("--token-file", str(token_file), "-B", "bundle_a")
        first = _decode(token_file.read_text())
        if previous:
            token_file.write_text(previous)

        self._run("--token-file", str(token_file), "-B", "bundle_b")

        assert _decode(token_file.read_text())["sub"] != first["sub"]

    @mock.patch.object(
        dag_processor_token_command.time,
        "sleep",
        autospec=True,
        side_effect=[None, None, None, _StopRotation],
    )
    @mock.patch.object(
        dag_processor_token_command,
        "write_token_file",
        autospec=True,
        side_effect=[None, OSError("disk full"), OSError("disk full"), None],
    )
    def test_rotation_retries_a_failed_write_after_the_first_token(self, _, mock_sleep, tmp_path):
        with pytest.raises(_StopRotation):
            self._run("--token-file", str(tmp_path / "token"), "--valid-for", "120", "--rotate")

        assert [call.args[0] for call in mock_sleep.call_args_list] == [60, 2, 4, 60]

    @mock.patch.object(dag_processor_token_command.time, "sleep", autospec=True)
    @mock.patch.object(
        dag_processor_token_command, "write_token_file", autospec=True, side_effect=OSError("disk full")
    )
    def test_first_write_failure_is_not_retried(self, _, mock_sleep, tmp_path):
        with pytest.raises(OSError, match="disk full"):
            self._run("--token-file", str(tmp_path / "token"), "--rotate")

        mock_sleep.assert_not_called()


class TestWriteTokenFile:
    def test_creates_the_file_readable_only_by_its_owner(self, tmp_path):
        token_file = tmp_path / "token"

        dag_processor_token_command.write_token_file(token_file, "new")
        dag_processor_token_command.write_token_file(token_file, "newer")

        assert token_file.read_text() == "newer"
        assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
        assert [path.name for path in tmp_path.iterdir()] == ["token"]

    def test_keeps_the_mode_and_group_of_the_replaced_file(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")
        token_file.chmod(0o640)

        with mock.patch.object(dag_processor_token_command.os, "fchown", autospec=True) as mock_fchown:
            dag_processor_token_command.write_token_file(token_file, "new")

        assert stat.S_IMODE(token_file.stat().st_mode) == 0o640
        mock_fchown.assert_called_once_with(mock.ANY, -1, os.stat(token_file).st_gid)

    @pytest.mark.parametrize("failing", ["fchmod", "fchown"])
    def test_keeps_the_previous_token_when_its_mode_or_group_cannot_be_kept(self, tmp_path, failing):
        token_file = tmp_path / "token"
        token_file.write_text("old")

        with (
            mock.patch.object(
                dag_processor_token_command.os, failing, autospec=True, side_effect=PermissionError
            ),
            pytest.raises(PermissionError),
        ):
            dag_processor_token_command.write_token_file(token_file, "new")

        assert token_file.read_text() == "old"
        assert [path.name for path in tmp_path.iterdir()] == ["token"]

    @pytest.mark.skipif(not os.path.isdir("/dev/fd"), reason="open descriptors cannot be listed")
    def test_failures_do_not_leak_descriptors(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")
        open_before = len(os.listdir("/dev/fd"))

        with mock.patch.object(
            dag_processor_token_command.os, "fchown", autospec=True, side_effect=PermissionError
        ):
            for _ in range(20):
                with pytest.raises(PermissionError):
                    dag_processor_token_command.write_token_file(token_file, "new")

        assert len(os.listdir("/dev/fd")) == open_before

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
