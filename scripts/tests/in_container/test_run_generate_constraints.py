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
from pathlib import Path
from types import SimpleNamespace

import pytest
import run_generate_constraints as m


def _config(latest: Path, current: Path, constraints_dir: Path | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        latest_constraints_file=latest,
        current_constraints_file=current,
        constraints_dir=constraints_dir if constraints_dir is not None else latest.parent,
        python="3.10",
    )


class TestReadProviderVersionsFromConstraints:
    def test_extracts_only_providers_and_strips_markers(self, tmp_path):
        constraints = tmp_path / "constraints.txt"
        constraints.write_text(
            "requests==2.0.0\n"
            "apache-airflow-providers-amazon==8.0.0\n"
            "apache-airflow-providers-http==4.0.0; python_version < '3.11'\n"
            "  apache-airflow-providers-google==10.0.0  \n"
            "apache-airflow-providers-broken-no-version\n"
        )
        assert m._read_provider_versions_from_constraints(constraints) == {
            "apache-airflow-providers-amazon": "8.0.0",
            "apache-airflow-providers-http": "4.0.0",
            "apache-airflow-providers-google": "10.0.0",
        }


class TestCheckProvidersNotDowngraded:
    def test_exits_and_writes_slack_payload_when_a_provider_is_downgraded(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SLACK_CHANNEL", raising=False)
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "apache/airflow")
        monkeypatch.setenv("GITHUB_RUN_ID", "12345")
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\napache-airflow-providers-google==10.0.0\n")
        # amazon is downgraded, google is upgraded.
        current.write_text(
            "apache-airflow-providers-amazon==7.5.0\napache-airflow-providers-google==10.1.0\n"
        )
        with pytest.raises(SystemExit) as exc_info:
            m.check_providers_not_downgraded(_config(latest, current, constraints_dir=tmp_path))
        assert exc_info.value.code == 1
        payload = json.loads((tmp_path / "provider-downgrade-slack-message.json").read_text())
        assert payload["channel"] == "internal-airflow-ci-cd"
        text_blocks = json.dumps(payload["blocks"])
        assert "apache-airflow-providers-amazon" in text_blocks
        assert "apache-airflow-providers-google" not in text_blocks
        assert "https://github.com/apache/airflow/actions/runs/12345" in text_blocks

    def test_slack_channel_is_overridable_via_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SLACK_CHANNEL", "some-other-channel")
        m.write_provider_downgrade_slack_message(
            _config(tmp_path / "latest.txt", tmp_path / "current.txt", constraints_dir=tmp_path),
            [("apache-airflow-providers-amazon", "8.0.0", "7.5.0")],
        )
        payload = json.loads((tmp_path / "provider-downgrade-slack-message.json").read_text())
        assert payload["channel"] == "some-other-channel"

    def test_passes_when_no_provider_is_downgraded(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\n")
        current.write_text(
            "apache-airflow-providers-amazon==8.1.0\napache-airflow-providers-google==10.0.0\n"
        )
        m.check_providers_not_downgraded(_config(latest, current))

    def test_passes_when_provider_removed_from_current(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\n")
        current.write_text("apache-airflow-providers-google==10.0.0\n")
        m.check_providers_not_downgraded(_config(latest, current))

    def test_skips_when_latest_file_missing(self, tmp_path):
        current = tmp_path / "current.txt"
        current.write_text("apache-airflow-providers-amazon==7.0.0\n")
        m.check_providers_not_downgraded(_config(tmp_path / "missing.txt", current))

    def test_skips_uncomparable_versions_without_error(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==not-a-version\n")
        current.write_text("apache-airflow-providers-amazon==also-bad\n")
        m.check_providers_not_downgraded(_config(latest, current))
