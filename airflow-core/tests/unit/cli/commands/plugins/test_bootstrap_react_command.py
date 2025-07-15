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

import io
import json
from contextlib import redirect_stdout

import pytest

from airflow.cli.commands.plugins import bootstrap_react_command

pytestmark = pytest.mark.db_test


class MockArgs:
    """Mock args object for testing."""

    def __init__(self, name: str, dir: str | None = None, verbose: bool = False):
        self.name = name
        self.dir = dir
        self.verbose = verbose


class TestBootstrapReactCommand:
    @pytest.mark.parametrize(
        "project_name,should_succeed",
        [
            # Valid names
            ("my-plugin", True),
            ("my_plugin", True),
            ("MyPlugin", True),
            ("plugin123", True),
            ("my-awesome_plugin", True),
            ("test_plugin_v2", True),
            ("my@plugin", False),  # Contains special character @
            ("", False),  # Empty string
        ],
    )
    def test_project_name_validation(self, tmp_path, project_name, should_succeed):
        """Test that project name validation works correctly for various inputs."""
        args = MockArgs(name=project_name, dir=str(tmp_path / "test_project"))

        if should_succeed:
            bootstrap_react_command.bootstrap_react_plugin(args)
            assert (tmp_path / "test_project").exists()
        else:
            # Should raise SystemExit due to validation failure
            with pytest.raises(SystemExit):
                bootstrap_react_command.bootstrap_react_plugin(args)

    def test_bootstrap_react_plugin_creates_project_structure(self, tmp_path):
        """Test that bootstrap creates the correct project structure with proper content."""
        project_name = "my-awesome-plugin"
        target_dir = tmp_path / "test_project"

        args = MockArgs(name=project_name, dir=str(target_dir))

        with redirect_stdout(io.StringIO()) as temp_stdout:
            bootstrap_react_command.bootstrap_react_plugin(args)
            stdout = temp_stdout.getvalue()

        # Check success message
        assert "Successfully created my-awesome-plugin!" in stdout
        assert "pnpm install" in stdout
        assert "pnpm dev" in stdout

        # Check directory structure was created
        assert target_dir.exists()
        assert (target_dir / "package.json").exists()
        assert (target_dir / "src").exists()
        assert (target_dir / "README.md").exists()

        # Check template variables were replaced in package.json
        package_json_content = (target_dir / "package.json").read_text()
        package_data = json.loads(package_json_content)
        assert package_data["name"] == project_name

        # Check that some source files exist (structure may vary based on real template)
        src_dir = target_dir / "src"
        assert src_dir.exists()
        assert len(list(src_dir.glob("*"))) > 0  # Has some files in src

        # Check README.md was processed
        readme_content = (target_dir / "README.md").read_text()
        assert project_name in readme_content  # Project name should be in README

    def test_bootstrap_react_plugin_fails_with_existing_directory(self, tmp_path):
        """Test that bootstrap fails when target directory already exists."""
        project_name = "existing-plugin"
        target_dir = tmp_path / "existing_dir"
        target_dir.mkdir()  # Create directory that already exists

        args = MockArgs(name=project_name, dir=str(target_dir))

        with pytest.raises(SystemExit):
            bootstrap_react_command.bootstrap_react_plugin(args)
