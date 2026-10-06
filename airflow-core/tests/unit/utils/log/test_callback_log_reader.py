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

from unittest.mock import patch

import pytest

from airflow.utils.log.callback_log_reader import read_callback_log, validate_log_path_component

from tests_common.test_utils.config import conf_vars


class TestValidateLogPathComponent:
    @pytest.mark.parametrize("component", ["my_dag", "manual__2024-01-01T00:00:00+00:00", "abc.123~x@y"])
    def test_valid_components_pass(self, component):
        assert validate_log_path_component(component) == component

    @pytest.mark.parametrize("component", ["..", ".", "a/b", "a\\b", "", "a b", "../etc"])
    def test_unsafe_components_raise(self, component):
        with pytest.raises(ValueError, match="Invalid log path component"):
            validate_log_path_component(component)


class TestReadCallbackLog:
    def test_no_logs_found_yields_message(self, tmp_path):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            msgs = list(read_callback_log("dag1", "run1", "cb1"))
        assert [m.event for m in msgs] == ["No callback logs found."]

    @pytest.mark.parametrize("prefix", ["executor_callbacks", "triggerer_callbacks"])
    def test_reads_local_logs(self, tmp_path, prefix):
        log_dir = tmp_path / prefix / "dag1" / "run1"
        log_dir.mkdir(parents=True)
        (log_dir / "cb1").write_text(f"{prefix} line\n")

        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            msgs = list(read_callback_log("dag1", "run1", "cb1"))

        assert any(m.event == f"{prefix} line" for m in msgs)

    def test_executor_path_preferred_over_triggerer(self, tmp_path):
        for prefix in ("executor_callbacks", "triggerer_callbacks"):
            log_dir = tmp_path / prefix / "dag1" / "run1"
            log_dir.mkdir(parents=True)
            (log_dir / "cb1").write_text(f"{prefix} line\n")

        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            events = [m.event for m in read_callback_log("dag1", "run1", "cb1")]

        assert "executor_callbacks line" in events
        assert "triggerer_callbacks line" not in events

    def test_symlink_escaping_log_folder_is_skipped(self, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        secret = outside / "secret"
        secret.write_text("secret data\n")

        log_folder = tmp_path / "logs"
        log_dir = log_folder / "executor_callbacks" / "dag1" / "run1"
        log_dir.mkdir(parents=True)
        (log_dir / "cb1").symlink_to(secret)

        with conf_vars({("logging", "base_log_folder"): str(log_folder)}):
            msgs = list(read_callback_log("dag1", "run1", "cb1"))

        assert [m.event for m in msgs] == ["No callback logs found."]

    def test_remote_logs_used_when_available(self, tmp_path):
        def one_stream():
            yield '{"event": "remote line"}\n'

        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            with patch(
                "airflow.utils.log.callback_log_reader._read_callback_remote_logs",
                return_value=(["s3://bucket/log"], [one_stream()]),
            ):
                msgs = list(read_callback_log("dag1", "run1", "cb1"))

        assert any(m.event == "remote line" for m in msgs)

    def test_path_traversal_components_rejected(self, tmp_path):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            with pytest.raises(ValueError, match="Invalid log path component"):
                list(read_callback_log("../etc", "run1", "cb1"))
