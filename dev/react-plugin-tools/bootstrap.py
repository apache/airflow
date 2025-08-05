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
Bootstrap React Plugin CLI Tool.

This script provides a command-line interface to create new React UI plugin
directories based on the airflow-core/ui project structure. It sets up all the
necessary configuration files, dependencies, and basic structure for development
with the same tooling as used in Airflow's core UI.
"""

from __future__ import annotations

import argparse
import re
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


def remove_apache_license_header(content: str, file_extension: str) -> str:
    """Remove Apache license header from file content based on file type."""
    if file_extension in [".ts", ".tsx", ".js", ".jsx"]:
        license_pattern = r"/\*!\s*\*\s*Licensed to the Apache Software Foundation.*?\*/\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    elif file_extension in [".md"]:
        license_pattern = r"<!--\s*Licensed to the Apache Software Foundation.*?-->\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    elif file_extension in [".html"]:
        license_pattern = r"<!--\s*Licensed to the Apache Software Foundation.*?-->\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)

    return content


def copy_template_files(template_dir: Path, project_path: Path, project_name: str) -> None:
    for item in template_dir.rglob("*"):
        if item.is_file():
            # Calculate relative path from template root
            rel_path = item.relative_to(template_dir)
            target_path = project_path / rel_path

            target_path.parent.mkdir(parents=True, exist_ok=True)

            with open(item, encoding="utf-8") as f:
                content = f.read()

            content = replace_template_variables(content, project_name)

            file_extension = item.suffix.lower()
            content = remove_apache_license_header(content, file_extension)

            with open(target_path, "w", encoding="utf-8") as f:
                f.write(content)

            print(f"  Created: {rel_path}")


def bootstrap_react_plugin(args) -> None:
    """Bootstrap a new React plugin project."""
    project_name = args.name
    target_dir = args.dir if args.dir else project_name

    project_path = Path(target_dir).resolve()
    template_dir = get_template_dir()

    if project_path.exists():
        print(f"Error: Directory '{project_path}' already exists!")
        sys.exit(1)

    if not project_name.replace("-", "").replace("_", "").isalnum():
        print("Error: Project name should only contain letters, numbers, hyphens, and underscores")
        sys.exit(1)

    print(f"Creating React plugin project: {project_name}")
    print(f"Target directory: {project_path}")
    print(f"Template directory: {template_dir}")

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
        if project_path.exists():
            shutil.rmtree(project_path)
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Bootstrap a new React UI plugin project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bootstrap.py my-plugin
  python bootstrap.py my-plugin --dir /path/to/projects/my-plugin

This will create a new React project with all the necessary configuration
files, dependencies, and structure needed for Airflow plugin development.
        """,
    )

    parser.add_argument(
        "name",
        help="Name of the React plugin project (letters, numbers, hyphens, and underscores only)",
    )

    parser.add_argument(
        "--dir",
        "-d",
        help="Target directory for the project (defaults to project name)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    try:
        bootstrap_react_plugin(args)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
