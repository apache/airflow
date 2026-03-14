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

from airflow_breeze.utils.confirm import Answer


def test_generate_constraints_forwards_allow_missing_previous_constraints_file(monkeypatch):
    import airflow_breeze.commands.release_management_commands as module

    captured_shell_params = {}

    monkeypatch.setattr(module, "perform_environment_checks", lambda: None)
    monkeypatch.setattr(module, "check_remote_ghcr_io_commands", lambda: None)
    monkeypatch.setattr(module, "fix_ownership_using_docker", lambda: None)
    monkeypatch.setattr(module, "cleanup_python_generated_files", lambda: None)
    monkeypatch.setattr(module, "user_confirm", lambda *args, **kwargs: Answer.YES)

    def fake_run_generate_constraints(shell_params, output):
        captured_shell_params["value"] = shell_params
        return 0, "constraints-source-providers:3.14"

    monkeypatch.setattr(module, "run_generate_constraints", fake_run_generate_constraints)

    module.generate_constraints.callback(
        airflow_constraints_mode="constraints-source-providers",
        allow_missing_previous_constraints_file=True,
        debug_resources=False,
        github_repository="apache/airflow",
        parallelism=1,
        python="3.14",
        python_versions="3.14",
        run_in_parallel=False,
        skip_cleanup=False,
        use_uv=True,
    )

    assert captured_shell_params["value"].allow_missing_previous_constraints_file is True
