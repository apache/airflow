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

import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "in_container" / "run_generate_constraints.py"


@pytest.fixture
def constraints_module(tmp_path, monkeypatch):
    module_name = "test_run_generate_constraints_module"
    sys.modules.pop(module_name, None)

    # run_generate_constraints.py reads provider_dependencies.json at module level.
    # Patch AIRFLOW_ROOT_PATH before exec_module so the `from in_container_utils import`
    # picks up the temp path and the read_text() call succeeds.
    fake_root = tmp_path / "airflow_root"
    (fake_root / "generated").mkdir(parents=True)
    (fake_root / "generated" / "provider_dependencies.json").write_text("{}")

    import in_container_utils

    monkeypatch.setattr(in_container_utils, "AIRFLOW_ROOT_PATH", fake_root)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def temp_constraints_dir(tmp_path, constraints_module, monkeypatch):
    monkeypatch.setattr(constraints_module.ConfigParams, "constraints_dir", property(lambda self: tmp_path))
    return tmp_path


def _config(constraints_module, *, allow_missing_previous_constraints_file: bool):
    return constraints_module.ConfigParams(
        airflow_constraints_mode="constraints-source-providers",
        allow_missing_previous_constraints_file=allow_missing_previous_constraints_file,
        constraints_github_repository="apache/airflow",
        default_constraints_branch="constraints-main",
        github_actions=False,
        python="3.14",
    )


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(f"{status_code} failure", response=response)


def test_download_latest_constraint_file_skips_missing_baseline_when_allowed(
    constraints_module, monkeypatch, temp_constraints_dir
):
    config_params = _config(constraints_module, allow_missing_previous_constraints_file=True)
    response = Mock()
    response.raise_for_status.side_effect = _http_error(404)
    mock_console = Mock()

    monkeypatch.setattr(constraints_module, "console", mock_console)
    monkeypatch.setattr(constraints_module.requests, "get", Mock(return_value=response))
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    assert constraints_module.download_latest_constraint_file(config_params) is False
    assert not config_params.latest_constraints_file.exists()
    assert any(
        "--allow-missing-previous-constraints-file" in str(call.args[0])
        for call in mock_console.print.call_args_list
    )


def test_download_latest_constraint_file_raises_with_guidance_without_override(
    constraints_module, monkeypatch, temp_constraints_dir
):
    config_params = _config(constraints_module, allow_missing_previous_constraints_file=False)
    response = Mock()
    response.raise_for_status.side_effect = _http_error(404)
    mock_console = Mock()

    monkeypatch.setattr(constraints_module, "console", mock_console)
    monkeypatch.setattr(constraints_module.requests, "get", Mock(return_value=response))
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(requests.HTTPError):
        constraints_module.download_latest_constraint_file(config_params)

    assert any(
        "--allow-missing-previous-constraints-file" in str(call.args[0])
        for call in mock_console.print.call_args_list
    )


def test_generate_constraints_source_providers_skips_diff_without_previous_file(
    constraints_module, monkeypatch, temp_constraints_dir
):
    config_params = _config(constraints_module, allow_missing_previous_constraints_file=True)
    mock_console = Mock()
    diff_constraints = Mock()

    monkeypatch.setattr(constraints_module, "console", mock_console)
    monkeypatch.setattr(
        constraints_module,
        "freeze_distributions_to_file",
        lambda _config_params, constraints_file, _distributions=None: constraints_file.write(
            "example==1.0.0\n"
        ),
    )
    monkeypatch.setattr(constraints_module, "download_latest_constraint_file", Mock(return_value=False))
    monkeypatch.setattr(constraints_module, "diff_constraints", diff_constraints)

    constraints_module.generate_constraints_source_providers(config_params)

    assert config_params.current_constraints_file.exists()
    diff_constraints.assert_not_called()
    assert any("Skipping constraints diff" in str(call.args[0]) for call in mock_console.print.call_args_list)
