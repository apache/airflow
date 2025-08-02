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
import argparse
import re
import shutil
import sys
from pathlib import Path

def get_template_dir() -> Path:
    script_dir = Path(__file__).parent
    template_dir = script_dir / "react_plugin_template"

    if not template_dir.exists():
        print(f"Error: Template directory not found at {template_dir}")
        sys.exit(1)

    return template_dir

def replace_template_variables(content: str, project_name: str) -> str:
    return content.replace("{{PROJECT_NAME}}", project_name)

def remove_apache_license_header(content: str, file_extension: str) -> str:
    if file_extension in [".ts", ".tsx", ".js", ".jsx"]:
        license_pattern = r"/\*!\s*\*\s*Licensed to the Apache Software Foundation.*?\*/\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    elif file_extension in [".md", ".html"]:
        license_pattern = r"<!--\s*Licensed to the Apache Software Foundation.*?-->\s*"
        content = re.sub(license_pattern, "", content, flags=re.DOTALL)
    return content

def copy_and_process_file(src: Path, dest: Path, project_name: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    with open(src, encoding="utf-8") as f:
        content = f.read()
    
    content = replace_template_variables(content, project_name)
    content = remove_apache_license_header(content, src.suffix.lower())
    
    with open(dest, "w", encoding="utf-8") as f:
        f.write(content)

def copy_directory_recursive(src_dir: Path, dest_dir: Path, project_name: str, skip_dirs: set = None) -> None:
    """Recursively copy directory while processing files and skipping specified directories."""
    if skip_dirs is None:
        skip_dirs = set()
    
    for item in src_dir.iterdir():
        # Skip directories that should be excluded
        if item.is_dir() and item.name in skip_dirs:
            print(f"✗ Skipping directory: {item.name}")
            continue
            
        dest_item = dest_dir / item.name
        
        if item.is_dir():
            dest_item.mkdir(parents=True, exist_ok=True)
            copy_directory_recursive(item, dest_item, project_name, skip_dirs)
        else:
            # Process text files, copy binary files as-is
            try:
                copy_and_process_file(item, dest_item, project_name)
            except UnicodeDecodeError:
                # Binary file, copy as-is
                shutil.copy2(item, dest_item)

def copy_template_files(template_dir: Path, project_path: Path, project_name: str, include_ai_rules: bool) -> None:
    
    # Determine which directories to skip
    skip_dirs = set()
    if not include_ai_rules:
        skip_dirs.add("ai_agents_rules")
    
    # Copy all files and directories, skipping excluded ones
    copy_directory_recursive(template_dir, project_path, project_name, skip_dirs)
    
    # Provide feedback
    if include_ai_rules:
        ai_src = template_dir / "ai_agents_rules"
        if ai_src.exists():
            print("✓ Copied AI rules folder")
        else:
            print("⚠ Warning: ai_agents_rules folder not found in template")
    else:
        print("✗ Skipped AI rules folder")

def bootstrap_react_plugin(args) -> None:
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

    # AI rules prompt
    ai_rules_answer = input("Include AI coding rules? [y/N]: ").strip().lower()
    include_ai_rules = ai_rules_answer in ("y", "yes")

    print(f"\nCreating React plugin: {project_name}")
    print(f"Location: {project_path}")
    print(f"Include AI rules: {'YES' if include_ai_rules else 'NO'}")

    project_path.mkdir(parents=True, exist_ok=True)

    try:
        print("\nCopying files...")
        copy_template_files(template_dir, project_path, project_name, include_ai_rules)

        print(f"\n✅ SUCCESS: Created {project_name}")
        print("\nNext steps:")
        print(f"  cd {target_dir}")
        print("  pnpm install")
        print("  pnpm dev")
        
        if include_ai_rules:
            print("\n🤖 AI rules location: ai_agents_rules/")
            print("For Cursor: Move to .cursor/rules/")
            print("For VSCode: Move to .github/copilot/")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        if project_path.exists():
            shutil.rmtree(project_path)
        sys.exit(1)

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Create a new React plugin project",
        epilog="Example: python bootstrap.py my-plugin --dir ./projects"
    )
    parser.add_argument(
        "name",
        help="Project name (letters, numbers, hyphens, underscores only)"
    )
    parser.add_argument(
        "--dir", "-d",
        help="Target directory (defaults to project name)"
    )
    args = parser.parse_args()

    try:
        bootstrap_react_plugin(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()