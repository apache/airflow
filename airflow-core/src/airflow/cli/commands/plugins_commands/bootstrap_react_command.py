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
Bootstrap React Plugin Directory Tool.

This module provides CLI commands to create new React UI plugin directory based
on the airflow-core/ui project structure. It sets up all the necessary configuration
files, dependencies, and basic structure for development with the same tooling as
used in Airflow's core UI.
"""

from __future__ import annotations

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
        # Remove JavaScript/TypeScript style license header
        license_pattern = r"/\*!\s*\*\s*Licensed to the Apache Software Foundation.*?\*/\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    elif file_extension in [".md"]:
        # Remove HTML/Markdown style license header
        license_pattern = r"<!--\s*Licensed to the Apache Software Foundation.*?-->\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    elif file_extension in [".html"]:
        # Remove HTML style license header
        license_pattern = r"<!--\s*Licensed to the Apache Software Foundation.*?-->\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)

    return content


def copy_template_files(template_dir: Path, project_path: Path, project_name: str) -> None:
    """Copy template files to the project directory, replacing variables and removing licenses."""
    for item in template_dir.rglob("*"):
        if item.is_file() or item.is_symlink():
            # Calculate relative path from template root
            rel_path = item.relative_to(template_dir)
            target_path = project_path / rel_path

            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Handle symlinks by resolving them and copying the actual content
            if item.is_symlink():
                # Resolve the symlink to get the actual file/directory
                resolved_item = item.resolve()

                if resolved_item.is_file():
                    # Copy single file from symlinked location
                    try:
                        with open(resolved_item, encoding="utf-8") as f:
                            content = f.read()

                        # Replace template variables and remove licenses
                        content = replace_template_variables(content, project_name)
                        file_extension = resolved_item.suffix.lower()
                        content = remove_apache_license_header(content, file_extension)

                        with open(target_path, "w", encoding="utf-8") as f:
                            f.write(content)
                    except UnicodeDecodeError:
                        # Handle binary files by copying directly
                        shutil.copy2(resolved_item, target_path)
                elif resolved_item.is_dir():
                    # Copy entire directory tree, resolving all symlinks
                    shutil.copytree(
                        resolved_item,
                        target_path,
                        symlinks=False,  # Resolve symlinks
                        dirs_exist_ok=True,
                    )
                    # Process all files in the copied directory for template variables and license removal
                    _process_copied_directory(target_path, project_name)

                print(f"  Created: {rel_path} (resolved from symlink)")
                continue

            # Handle regular files
            try:
                with open(item, encoding="utf-8") as f:
                    content = f.read()
            except UnicodeDecodeError:
                # Handle binary files by copying directly
                shutil.copy2(item, target_path)
                continue

            # Replace template variables
            content = replace_template_variables(content, project_name)

            # Remove Apache license headers
            file_extension = item.suffix.lower()
            content = remove_apache_license_header(content, file_extension)

            # Write to target location
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(content)

            print(f"  Created: {rel_path}")


def _process_copied_directory(directory: Path, project_name: str) -> None:
    """Process all files in a copied directory for template variables and license removal."""
    for item in directory.rglob("*"):
        if item.is_file():
            try:
                with open(item, encoding="utf-8") as f:
                    content = f.read()

                # Replace template variables
                original_content = content
                content = replace_template_variables(content, project_name)

                # Remove Apache license headers
                file_extension = item.suffix.lower()
                content = remove_apache_license_header(content, file_extension)

                # Only write if content changed
                if content != original_content:
                    with open(item, "w", encoding="utf-8") as f:
                        f.write(content)

            except UnicodeDecodeError:
                # Skip binary files
                continue


def bootstrap_react_plugin(args) -> None:
    """Bootstrap a new React plugin project."""
    project_name = args.name
    target_dir = args.dir if args.dir else project_name

    project_path = Path(target_dir).resolve()
    template_dir = get_template_dir()

    # Check if directory already exists
    if project_path.exists():
        print(f"Error: Directory '{project_path}' already exists!")
        sys.exit(1)

    # Validate project name
    if not project_name.replace("-", "").replace("_", "").isalnum():
        print("Error: Project name should only contain letters, numbers, hyphens, and underscores")
        sys.exit(1)

    print(f"Creating React plugin project: {project_name}")
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
        print("  yarn install")
        print("  yarn dev")
        print("\nHappy coding! 🚀")

    except Exception as e:
        print(f"Error creating project: {e}")
        # Clean up on error
        if project_path.exists():
            shutil.rmtree(project_path)
        sys.exit(1)
