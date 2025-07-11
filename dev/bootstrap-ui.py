#!/usr/bin/env python3
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

"""
Bootstrap React UI Directory Tool

This tool creates a new React UI directory based on the airflow-core/ui project structure.
It sets up all the necessary configuration files, dependencies, and basic structure for
development with the same tooling as used in Airflow's core UI.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def get_template_dir() -> Path:
    """Get the template directory path."""
    script_dir = Path(__file__).parent
    template_dir = script_dir / "react_plugin_template"

    if not template_dir.exists():
        print(f"Error: Template directory not found at {template_dir}")
        sys.exit(1)

    return template_dir


def replace_template_variables(content: str, project_name: str) -> str:
    """Replace template variables in file content."""
    return content.replace("{{PROJECT_NAME}}", project_name)


def copy_template_files(template_dir: Path, project_path: Path, project_name: str) -> None:
    """Copy template files to the project directory, replacing variables."""
    for item in template_dir.rglob("*"):
        if item.is_file():
            # Calculate relative path from template root
            rel_path = item.relative_to(template_dir)
            target_path = project_path / rel_path

            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Read file content
            try:
                with open(item, encoding="utf-8") as f:
                    content = f.read()
            except UnicodeDecodeError:
                # Handle binary files by copying directly
                shutil.copy2(item, target_path)
                continue

            # Replace template variables
            content = replace_template_variables(content, project_name)

            # Write to target location
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(content)

            print(f"  Created: {rel_path}")


def bootstrap_ui_project(project_name: str, target_dir: str | None = None) -> None:
    """Bootstrap a new React UI project."""
    if target_dir is None:
        target_dir = project_name

    project_path = Path(target_dir).resolve()
    template_dir = get_template_dir()

    # Check if directory already exists
    if project_path.exists():
        print(f"Error: Directory '{project_path}' already exists!")
        sys.exit(1)

    print(f"Creating React UI project: {project_name}")
    print(f"Target directory: {project_path}")
    print(f"Template directory: {template_dir}")

    # Create project directory
    project_path.mkdir(parents=True, exist_ok=True)

    try:
        # Copy template files
        print("Copying template files...")
        copy_template_files(template_dir, project_path, project_name)

        print(f"\n✅ Successfully created {project_name}!")
        print("\nNext steps:")
        print(f"  cd {target_dir}")
        print("  pnpm install")
        print("  pnpm dev")
        print("\nHappy coding! 🚀")

    except Exception as e:
        print(f"Error creating project: {e}")
        # Clean up on error
        if project_path.exists():
            shutil.rmtree(project_path)
        sys.exit(1)


def main():
    """Main entry point for the bootstrap script."""
    parser = argparse.ArgumentParser(
        description="Bootstrap a new React UI project based on Airflow's core UI configuration"
    )
    parser.add_argument("project_name", help="Name of the project to create")
    parser.add_argument("-d", "--dir", help="Target directory (defaults to project name)", default=None)

    args = parser.parse_args()

    # Validate project name
    if not args.project_name.replace("-", "").replace("_", "").isalnum():
        print("Error: Project name should only contain letters, numbers, hyphens, and underscores")
        sys.exit(1)

    bootstrap_ui_project(args.project_name, args.dir)


if __name__ == "__main__":
    main()
