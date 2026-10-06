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
    @pytest.fixture(autouse=True)
    def log_folder(self, tmp_path):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path / "logs")}):
            yield tmp_path / "logs"

    def test_no_logs_found_yields_message(self):
        assert [m.event for m in read_callback_log("dag1", "run1", "cb1")] == ["No callback logs found."]

    @pytest.mark.parametrize(
        ("prefixes", "expected"),
        [
            (["executor_callbacks"], "executor_callbacks line"),
            (["triggerer_callbacks"], "triggerer_callbacks line"),
            (["executor_callbacks", "triggerer_callbacks"], "executor_callbacks line"),
        ],
        ids=["executor", "triggerer", "executor_preferred"],
    )
    def test_reads_local_logs(self, log_folder, prefixes, expected):
        for prefix in prefixes:
            log_dir = log_folder / prefix / "dag1" / "run1"
            log_dir.mkdir(parents=True)
            (log_dir / "cb1").write_text(f"{prefix} line\n")

        events = [m.event for m in read_callback_log("dag1", "run1", "cb1")]

        assert [event for event in events if event.endswith(" line")] == [expected]

    def test_symlink_escaping_log_folder_is_skipped(self, log_folder, tmp_path):
        secret = tmp_path / "secret"
        secret.write_text("secret data\n")
        log_dir = log_folder / "executor_callbacks" / "dag1" / "run1"
        log_dir.mkdir(parents=True)
        (log_dir / "cb1").symlink_to(secret)

        assert [m.event for m in read_callback_log("dag1", "run1", "cb1")] == ["No callback logs found."]

    def test_remote_logs_used_when_available(self):
        def one_stream():
            yield '{"event": "remote line"}\n'

        with patch(
            "airflow.utils.log.callback_log_reader._read_callback_remote_logs",
            return_value=(["s3://bucket/log"], [one_stream()]),
        ):
            msgs = list(read_callback_log("dag1", "run1", "cb1"))

        assert any(m.event == "remote line" for m in msgs)

    def test_path_traversal_components_rejected(self):
        with pytest.raises(ValueError, match="Invalid log path component"):
            list(read_callback_log("../etc", "run1", "cb1"))
