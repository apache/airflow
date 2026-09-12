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

import subprocess

from ci.prek import check_supervisor_schemas_versions as hook


def test_get_clean_git_env_removes_git_local_env_vars(monkeypatch):
    monkeypatch.setenv("GIT_INDEX_FILE", "/tmp/hook-index")
    monkeypatch.setenv("GIT_DIR", "/tmp/git-dir")
    monkeypatch.setenv("AIRFLOW_KEEP_ME", "yes")

    def fake_run(cmd, **kwargs):
        assert cmd == ["git", "rev-parse", "--local-env-vars"]
        assert kwargs == {"capture_output": True, "text": True, "check": True}
        return subprocess.CompletedProcess(cmd, 0, stdout="GIT_INDEX_FILE\nGIT_DIR\n")

    monkeypatch.setattr(hook.subprocess, "run", fake_run)

    env = hook.get_clean_git_env()

    assert "GIT_INDEX_FILE" not in env
    assert "GIT_DIR" not in env
    assert env["AIRFLOW_KEEP_ME"] == "yes"
