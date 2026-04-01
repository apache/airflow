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

import textwrap
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def write_python_file(tmp_path):
    """Factory fixture: write dedented Python code to a temp .py file and return its Path."""

    def _write(code: str) -> Path:
        path = tmp_path / "code.py"
        path.write_text(textwrap.dedent(code))
        return path

    return _write


@pytest.fixture
def write_text_file(tmp_path):
    """Factory fixture: write text content to a temp file and return its Path."""

    def _write(content: str) -> Path:
        path = tmp_path / "content.txt"
        path.write_text(content)
        return path

    return _write


@pytest.fixture
def write_workflow_file(tmp_path):
    """Factory fixture: write a workflow dict as YAML to a temp file and return its Path."""

    def _write(content: dict) -> Path:
        path = tmp_path / "workflow.yml"
        path.write_text(yaml.dump(content))
        return path

    return _write


@pytest.fixture
def create_provider_tree(tmp_path):
    """Factory fixture: create a directory tree with provider.yaml and return a file path inside it."""

    def _create(relative_path: str) -> Path:
        provider_dir = tmp_path / relative_path
        provider_dir.mkdir(parents=True, exist_ok=True)
        (provider_dir / "provider.yaml").touch()
        hooks_dir = provider_dir / "hooks"
        hooks_dir.mkdir(exist_ok=True)
        test_file = hooks_dir / "hook.py"
        test_file.touch()
        return test_file

    return _create
