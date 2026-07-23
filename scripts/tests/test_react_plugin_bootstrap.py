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
from types import SimpleNamespace
from unittest import mock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[2] / "dev" / "react-plugin-tools" / "bootstrap.py"


@pytest.fixture
def bootstrap_module():
    module_name = "test_react_plugin_bootstrap_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize(
    "answer,expected",
    [
        ("y", True),
        ("YES", True),
        (" yes ", True),
        ("", False),
        ("n", False),
        ("anything else", False),
    ],
)
@mock.patch("builtins.input")
def test_should_include_ai_agent_rules(mock_input, bootstrap_module, answer, expected):
    mock_input.return_value = answer

    assert bootstrap_module.should_include_ai_agent_rules() is expected


@pytest.mark.parametrize(("answer", "include_ai_agent_rules"), [("yes", True), ("", False)])
@mock.patch("builtins.input")
def test_bootstrap_conditionally_includes_ai_agent_rules(
    mock_input, bootstrap_module, tmp_path, monkeypatch, answer, include_ai_agent_rules
):
    mock_input.return_value = answer
    template_dir = tmp_path / "template"
    rules_dir = template_dir / bootstrap_module.AI_AGENT_RULES_DIR
    rules_dir.mkdir(parents=True)
    (template_dir / "README.md").write_text("project", encoding="utf-8")
    (rules_dir / "react.md").write_text("rules", encoding="utf-8")
    project_path = tmp_path / "project"
    get_template_dir = mock.create_autospec(bootstrap_module.get_template_dir, return_value=template_dir)
    monkeypatch.setattr(bootstrap_module, "get_template_dir", get_template_dir)

    bootstrap_module.bootstrap_react_plugin(SimpleNamespace(name="project", dir=project_path))

    assert (project_path / "README.md").is_file()
    assert (project_path / bootstrap_module.AI_AGENT_RULES_DIR / "react.md").exists() is (
        include_ai_agent_rules
    )
