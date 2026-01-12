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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
"""
Prek hook to verify and sync shared_distributions in pyproject.toml files.
Ensures only valid shared libraries are referenced and that they exist in the shared/ folder.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Callable
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # for common_prek_utils import
from common_prek_utils import AIRFLOW_ROOT_PATH, console, insert_documentation

SHARED_DIR = AIRFLOW_ROOT_PATH / "shared"


def find_pyproject_files_with_tool_airflow(root: Path) -> list[Path]:
    pyproject_files = []
    for pyproject in root.glob("**/pyproject.toml"):
        console.print(f"[bold blue]Checking:[/bold blue] {pyproject}", end="")
        try:
            with open(pyproject, "rb") as f:
                data = tomllib.load(f)
            if "tool" in data and "airflow" in data["tool"]:
                console.print("  [green]tool.airflow section found[/green]")
                pyproject_files.append(pyproject)
            else:
                console.print("  [yellow]tool.airflow section NOT found[/yellow]")
        except Exception as e:
            console.print(f"  [red]Error reading {pyproject}: {e}[/red]")
            continue
    return pyproject_files


def get_shared_distributions(pyproject_path: Path) -> list[str]:
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    return data.get("tool", {}).get("airflow", {}).get("shared_distributions", [])


def normalize_package_name_to_directory(package_name: str) -> str:
    """
    Normalize package name to directory name convention.
    Converts hyphens to underscores: 'secrets-masker' -> 'secrets_masker'
    """
    return package_name.replace("-", "_")


def verify_shared_distributions(shared_distributions: list[str], shared_dir: Path) -> list[str]:
    errors = []
    for dist in shared_distributions:
        console.print(f"  Checking shared distribution: [magenta]{dist}[/magenta]", end="")
        if not re.match(r"^apache-airflow-shared-.+", dist):
            errors.append(
                f"Invalid shared distribution name: {dist}. Must start with 'apache-airflow-shared-'."
            )
            console.print("    [red]Invalid name[/red]")
            continue
        subfolder = dist.replace("apache-airflow-shared-", "")
        # Normalize package name to directory convention (hyphens -> underscores)
        normalized_subfolder = normalize_package_name_to_directory(subfolder)
        if not (shared_dir / normalized_subfolder).is_dir():
            errors.append(f"Shared distribution '{dist}' does not correspond to a subfolder in 'shared/'.")
            console.print("    [red]NOK[/red]")
        else:
            console.print("    [green]OK[/green]")
    return errors


def find_shared_folder_in_src(pyproject_path: Path) -> Path | None:
    """
    Checks if a _shared directory exists anywhere under the src folder of the project.
    Returns the Path if found, or None if missing.
    """
    project_dir = pyproject_path.parent
    src_dir = project_dir / "src"
    if not src_dir.is_dir():
        console.print(
            f"  [red]The src directory not found in {project_dir}. Cannot check for _shared folder.[/red]"
        )
        return None
    console.print(f"  Searching for _shared directory under src: {src_dir}. ", end="")
    for path in src_dir.rglob("_shared"):
        if path.is_dir():
            console.print("[green]OK[/green]")
            console.print(f"  The _shared directory found: {path}")
            return path
    console.print("[red]Not found.[/red]")
    return None


def check_force_include(pyproject: Path, shared_distributions: list[str], shared_folder: Path) -> list[str]:
    """
    Checks that for each shared distribution, a proper force-include entry exists in tool.hatch.build.targets.sdist.force-include.
    If missing, adds it automatically to pyproject.toml.
    Returns a list of error messages for missing or incorrect entries.
    """
    errors: list[str] = []
    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        return [f"Error reading pyproject.toml: {e}"]
    force_include = (
        data.get("tool", {})
        .get("hatch", {})
        .get("build", {})
        .get("targets", {})
        .get("sdist", {})
        .get("force-include", {})
    )
    pyproject_text = pyproject.read_text()
    updated = False
    console.print(f"  Checking force-include entries in {pyproject} ", end="")
    for dist in shared_distributions:
        dist_name = dist.replace("apache-airflow-shared-", "")
        # Normalize package name to directory convention (hyphens -> underscores)
        normalized_dist_name = normalize_package_name_to_directory(dist_name)
        shared_src = f"../shared/{normalized_dist_name}/src/airflow_shared/{normalized_dist_name}"
        found = False
        for src, _ in force_include.items():
            if src == shared_src:
                found = True
                break
        if not found:
            # Add missing entry to pyproject.toml
            rel_dest = f"{shared_folder.relative_to(pyproject.parent)}/{normalized_dist_name}"
            entry = f'"{shared_src}" = "{rel_dest}"\n'
            # Find or create the [tool.hatch.build.targets.sdist.force-include] section
            section_header = "[tool.hatch.build.targets.sdist.force-include]"
            if section_header in pyproject_text:
                # Insert entry after section header
                lines = pyproject_text.splitlines(keepends=True)
                for i, line in enumerate(lines):
                    if line.strip() == section_header:
                        # Insert after header and any existing entries
                        insert_at = i + 1
                        while (
                            insert_at < len(lines)
                            and lines[insert_at].startswith("[") is False
                            and lines[insert_at].strip()
                        ):
                            insert_at += 1
                        lines.insert(insert_at, entry)
                        break
                pyproject_text = "".join(lines)
            else:
                # Add new section at the end
                pyproject_text += f"\n{section_header}\n{entry}"
            pyproject.write_text(pyproject_text)
            updated = True
            console.print(f"[yellow]Added missing force-include entry for {dist} in {pyproject}[/yellow]")
        else:
            console.print(f"{dist}: [green]OK[/green]")
    if updated:
        # Reload data for next checks if needed
        pass
    return errors


def get_workspace_pyproject_toml_files(main_pyproject_path: Path) -> list[Path]:
    """
    Parse the main pyproject.toml using tomllib to get the list of workspace project folders from [tool.uv.sources].
    """
    try:
        with open(main_pyproject_path, "rb") as f:
            data = tomllib.load(f)
        members = data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
        pyproject_toml_candidates = [AIRFLOW_ROOT_PATH / member / "pyproject.toml" for member in members]
        return [file for file in pyproject_toml_candidates if file.is_file()]
    except Exception as e:
        console.print(f"[red]Error reading workspace projects from {main_pyproject_path}: {e}[/red]")
        return []


def ensure_symlinks(shared_folder: Path, shared_distributions: list[str]) -> list[str]:
    """
    Ensure symlinks for shared distributions exist in the _shared folder. Create if missing.
    Returns a list of errors if symlink creation fails.
    """
    errors: list[str] = []
    for distribution in shared_distributions:
        subfolder = distribution.replace("apache-airflow-shared-", "")
        # Normalize package name to directory convention (hyphens -> underscores)
        normalized_subfolder = normalize_package_name_to_directory(subfolder)
        symlink_path = shared_folder / normalized_subfolder
        console.print(f"  Checking for symlink: [magenta]{normalized_subfolder}[/magenta].   ", end="")
        target_path = SHARED_DIR / normalized_subfolder / "src" / "airflow_shared" / normalized_subfolder
        # Make symlink relative
        rel_target_path = os.path.relpath(target_path, symlink_path.parent)
        if not symlink_path.exists():
            try:
                os.symlink(rel_target_path, symlink_path)
                console.print(f"[yellow]Created symlink: {symlink_path} -> {rel_target_path}[/yellow]")
            except Exception as e:
                errors.append(f"Failed to create symlink for {distribution}: {e}")
                console.print(
                    f"[red]Failed to create symlink: {symlink_path} -> {rel_target_path}: {e}[/red]"
                )
        elif not symlink_path.is_symlink() or symlink_path.resolve() != target_path.resolve():
            try:
                if symlink_path.exists():
                    if symlink_path.is_symlink():
                        symlink_path.unlink()
                    else:
                        errors.append(
                            f"{symlink_path} exists and is not a symlink. Please remove it manually."
                        )
                        console.print(
                            f"[red]{symlink_path} exists and is not a symlink. Please remove it manually.[/red]"
                        )
                        continue
                os.symlink(rel_target_path, symlink_path)
                console.print(f"[green]Fixed symlink: {symlink_path} -> {rel_target_path}[green]")
            except Exception as e:
                errors.append(f"Failed to fix symlink for {distribution}: {e}")
                console.print(f"[red]Failed to fix symlink: {symlink_path} -> {rel_target_path}: {e}[/red]")
        else:
            console.print(f"[green]OK: {symlink_path} -> {rel_target_path}[green]")
    return errors


def find_dependencies_array_range(lines: list[str]) -> tuple[int | None, int | None]:
    """
    Finds the start and end line indices of the dependencies array in a pyproject.toml file.
    Returns (dep_start, dep_end) or (None, None) if not found.
    """
    dep_start, dep_end = None, None
    for i, line in enumerate(lines):
        if re.match(r"^dependencies\s*=\s*\[", line.strip()):
            dep_start = i
            for j in range(i, len(lines)):
                if lines[j].strip() == "]":
                    dep_end = j
                    break
            break
    return dep_start, dep_end


def add_shared_dependencies_block(
    project_pyproject_path: Path, dep_end: int, header: str, footer: str, content: list[str], dist_name: str
) -> None:
    """
    Insert a shared dependencies block at the end of the dependencies array in pyproject.toml.
    """
    lines = project_pyproject_path.read_text().splitlines(keepends=True)
    new_lines = (
        lines[:dep_end] + ["    " + header + "\n"] + content + ["    " + footer + "\n"] + lines[dep_end:]
    )
    project_pyproject_path.write_text("".join(new_lines))
    console.print(f"[yellow]Added shared dependencies for {dist_name} in {project_pyproject_path}[/yellow]")


def extract_existing_dependencies(project_pyproject_path: Path) -> set[str]:
    """
    Extract existing dependency names (without version constraints) from a pyproject.toml file.
    Returns a set of package names that are already in the main dependencies list.
    """
    try:
        with open(project_pyproject_path, "rb") as f:
            data = tomllib.load(f)

        deps = data.get("project", {}).get("dependencies", [])
        existing_deps = set()

        for dep in deps:
            # Extract package name (everything before >=, >, <, ==, etc.)
            # Handle cases like 'pendulum>=3.1.0', 'requests[security]>=2.0', etc.
            package_name = re.split(r"[<>=!]", dep)[0].strip()
            # Remove any extras like [security]
            package_name = re.split(r"\[", package_name)[0].strip()
            # Remove quotes
            package_name = package_name.strip("\"'")
            if package_name:
                existing_deps.add(package_name)

        return existing_deps
    except Exception as e:
        console.print(f"[red]Error extracting dependencies from {project_pyproject_path}: {e}[/red]")
        return set()


def filter_duplicate_dependencies(shared_deps: list[str], existing_deps: set[str]) -> list[str]:
    """
    Filter out shared dependencies that already exist in the consuming package.
    """
    filtered_deps = []
    for dep in shared_deps:
        package_name = re.split(r"[<>=!]", dep)[0].strip()
        package_name = re.split(r"\[", package_name)[0].strip()
        package_name = package_name.strip("\"'")

        if package_name not in existing_deps:
            filtered_deps.append(dep)
        else:
            console.print(f"[dim]  Skipping duplicate dependency: {dep}[/dim]")

    return filtered_deps


def get_all_shared_modules(shared_dir: Path) -> list[str]:
    """
    Get all shared module names from the shared/ directory.
    Returns list of package names like 'apache-airflow-shared-configuration'.
    """
    shared_modules = []
    if not shared_dir.exists():
        return shared_modules

    for item in shared_dir.iterdir():
        if item.is_dir() and not item.name.startswith(".") and item.name != "__pycache__":
            # Check if it has a pyproject.toml to confirm it's a valid package
            if (item / "pyproject.toml").exists():
                # Convert directory name to package name
                # e.g., 'secrets_masker' -> 'apache-airflow-shared-secrets-masker'
                package_name = f"apache-airflow-shared-{item.name.replace('_', '-')}"
                shared_modules.append(package_name)

    return sorted(shared_modules)


def find_and_sort_entries(
    file_text: str, section_marker: str, entry_prefix: str, stop_marker: str | None = None
) -> tuple[list[str], list[int], int]:
    """
    Find entries matching a prefix in a TOML section and return them with their indices.

    Args:
        file_text: The content of the file
        section_marker: The marker to identify the section (e.g., "members = [", "[tool.uv.sources]")
        entry_prefix: The prefix to match entries (e.g., '"shared/', 'apache-airflow-shared-')
        stop_marker: Optional marker to stop searching (e.g., "# Automatically generated provider")

    Returns:
        Tuple of (entries, indices, section_start_line)
    """
    lines = file_text.splitlines(keepends=True)
    entries = []
    indices = []
    section_start = -1

    for i, line in enumerate(lines):
        if section_marker in line:
            if section_marker.startswith("["):
                # For section headers like [tool.uv.sources]
                section_start = i
            elif "members = [" in section_marker:
                # Check if this is the workspace members section
                if "[tool.uv.workspace]" in "".join(lines[max(0, i - 5) : i + 1]):
                    section_start = i
            else:
                section_start = i

            if section_start >= 0:
                for j in range(section_start + 1, len(lines)):
                    if stop_marker and stop_marker in lines[j]:
                        break
                    if entry_prefix in lines[j]:
                        entries.append(lines[j])
                        indices.append(j)
                break

    return entries, indices, section_start


def sort_entries_in_section(file_text: str, entries: list[str], indices: list[int]) -> str:
    """
    Sort entries in place within a TOML section.

    Args:
        file_text: The content of the file
        entries: The list of entry lines to sort
        indices: The indices where these entries appear

    Returns:
        Updated file content with sorted entries
    """
    if not entries or entries == sorted(entries):
        return file_text

    lines = file_text.splitlines(keepends=True)
    sorted_entries = sorted(entries)
    for idx, new_entry in zip(indices, sorted_entries):
        lines[idx] = new_entry

    return "".join(lines)


def add_missing_entries_to_section(
    file_text: str,
    section_marker: str,
    missing_entries: list[str],
    entry_formatter: Callable[[str], str],
    insert_before_marker: str | None = None,
    find_last_prefix: str | None = None,
) -> tuple[str, bool]:
    """
    Add missing entries to a TOML section.

    Args:
        file_text: The content of the file
        section_marker: The marker to identify the section
        missing_entries: List of entries to add (sorted)
        entry_formatter: Function to format each entry as a line
        insert_before_marker: Optional marker to insert before
        find_last_prefix: Optional prefix to find last occurrence and insert after

    Returns:
        Tuple of (updated file content, was_updated)
    """
    if not missing_entries:
        return file_text, False

    lines = file_text.splitlines(keepends=True)
    insert_line = None

    for i, line in enumerate(lines):
        section_found = False
        if section_marker in line:
            if section_marker.startswith("["):
                section_found = True
            elif "members = [" in section_marker:
                if "[tool.uv.workspace]" in "".join(lines[max(0, i - 5) : i + 1]):
                    section_found = True
            elif "dev = [" in section_marker:
                section_found = True

            if section_found:
                # Find insertion point
                for j in range(i + 1, len(lines)):
                    if insert_before_marker and insert_before_marker in lines[j]:
                        insert_line = j
                        break
                    if find_last_prefix and find_last_prefix in lines[j]:
                        insert_line = j + 1
                break

    if insert_line is not None:
        for entry in sorted(missing_entries):
            lines.insert(insert_line, entry_formatter(entry))
            insert_line += 1
        return "".join(lines), True

    return file_text, False


def ensure_shared_in_workspace_and_dev(main_pyproject_path: Path, shared_dir: Path) -> list[str]:
    """
    Ensures all shared modules are in workspace members, [tool.uv.sources], and dev dependencies.
    Also ensures they are sorted alphabetically.
    Returns list of errors if any.
    """
    errors = []
    shared_modules = get_all_shared_modules(shared_dir)

    if not shared_modules:
        console.print("[yellow]No shared modules found in shared/ directory[/yellow]")
        return errors

    console.print(
        f"\n[bold blue]Found {len(shared_modules)} shared modules in shared/ directory:[/bold blue]"
    )
    for module in shared_modules:
        console.print(f"  - {module}")

    try:
        with open(main_pyproject_path, "rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        return [f"Error reading main pyproject.toml: {e}"]

    # Check workspace members
    workspace_members = data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    missing_in_workspace = []
    for module in shared_modules:
        # Convert package name to directory path
        # e.g., 'apache-airflow-shared-secrets-masker' -> 'shared/secrets_masker'
        dir_name = module.replace("apache-airflow-shared-", "").replace("-", "_")
        workspace_path = f"shared/{dir_name}"
        if workspace_path not in workspace_members:
            missing_in_workspace.append(workspace_path)

    # Check [tool.uv.sources]
    uv_sources = data.get("tool", {}).get("uv", {}).get("sources", {})
    missing_in_sources = [module for module in shared_modules if module not in uv_sources]

    # Check dev dependencies
    dev_deps = data.get("dependency-groups", {}).get("dev", [])
    missing_in_dev = [module for module in shared_modules if module not in dev_deps]

    # Report and fix missing/unsorted entries
    file_text = main_pyproject_path.read_text()
    updated = False

    # Handle workspace members
    if missing_in_workspace:
        console.print(
            f"\n[yellow]Missing {len(missing_in_workspace)} shared modules in workspace members:[/yellow]"
        )
        for path in missing_in_workspace:
            console.print(f"  - {path}")

        file_text, was_updated = add_missing_entries_to_section(
            file_text,
            "members = [",
            missing_in_workspace,
            lambda p: f'    "{p}",\n',
            insert_before_marker="# Automatically generated provider workspace members",
            find_last_prefix="shared/",
        )
        if was_updated:
            updated = True
            console.print("[green]Added missing workspace members[/green]")

    # Sort workspace members
    entries, indices, _ = find_and_sort_entries(
        file_text,
        "members = [",
        '"shared/',
        "# Automatically generated provider workspace members",
    )
    if entries and entries != sorted(entries):
        console.print("\n[yellow]Shared workspace members are not sorted, sorting them...[/yellow]")
        file_text = sort_entries_in_section(file_text, entries, indices)
        updated = True
        console.print("[green]Sorted shared workspace members[/green]")

    # Handle [tool.uv.sources]
    if missing_in_sources:
        console.print(
            f"\n[yellow]Missing {len(missing_in_sources)} shared modules in [tool.uv.sources]:[/yellow]"
        )
        for module in missing_in_sources:
            console.print(f"  - {module}")

        file_text, was_updated = add_missing_entries_to_section(
            file_text,
            "[tool.uv.sources]",
            missing_in_sources,
            lambda m: f"{m} = {{ workspace = true }}\n",
            insert_before_marker="# Automatically generated provider workspace items",
            find_last_prefix="apache-airflow-shared-",
        )
        if was_updated:
            updated = True
            console.print("[green]Added missing [tool.uv.sources] entries[/green]")

    # Sort [tool.uv.sources]
    entries, indices, _ = find_and_sort_entries(
        file_text,
        "[tool.uv.sources]",
        "apache-airflow-shared-",
        "# Automatically generated provider workspace items",
    )
    if entries and entries != sorted(entries):
        console.print("\n[yellow]Shared [tool.uv.sources] entries are not sorted, sorting them...[/yellow]")
        file_text = sort_entries_in_section(file_text, entries, indices)
        updated = True
        console.print("[green]Sorted shared [tool.uv.sources] entries[/green]")

    # Handle dev dependencies
    if missing_in_dev:
        console.print(f"\n[yellow]Missing {len(missing_in_dev)} shared modules in dev dependencies:[/yellow]")
        for module in missing_in_dev:
            console.print(f"  - {module}")

        file_text, was_updated = add_missing_entries_to_section(
            file_text,
            "dev = [",
            missing_in_dev,
            lambda m: f'    "{m}",\n',
            insert_before_marker=None,
            find_last_prefix="apache-airflow-shared-",
        )
        if was_updated:
            updated = True
            console.print("[green]Added missing dev dependencies[/green]")

    # Sort dev dependencies
    entries, indices, _ = find_and_sort_entries(file_text, "dev = [", '"apache-airflow-shared-', None)
    if entries and entries != sorted(entries):
        console.print("\n[yellow]Shared dev dependencies are not sorted, sorting them...[/yellow]")
        file_text = sort_entries_in_section(file_text, entries, indices)
        updated = True
        console.print("[green]Sorted shared dev dependencies[/green]")

    if updated:
        main_pyproject_path.write_text(file_text)
        console.print(f"\n[bold green]Updated {main_pyproject_path}[/bold green]")
    else:
        console.print(
            "\n[bold green]All shared modules are properly configured in main pyproject.toml[/bold green]"
        )

    return errors


def sync_shared_dependencies(project_pyproject_path: Path, shared_distributions: list[str]) -> None:
    """
    Synchronize dependencies from shared distributions into the project's pyproject.toml.
    Updates or inserts blocks marked with start/end comments for each shared distribution using insert_documentation.
    Adds the block if missing. Skips dependencies that already exist in the main dependencies list.
    """
    # Extract existing dependencies to avoid duplicates
    existing_deps = extract_existing_dependencies(project_pyproject_path)

    for dist in shared_distributions:
        dist_name = dist.replace("apache-airflow-shared-", "")
        # Normalize package name to directory convention (hyphens -> underscores)
        normalized_dist_name = normalize_package_name_to_directory(dist_name)
        console.print(
            f"  Synchronizing shared dependencies for [magenta]{dist_name}[/magenta] in {project_pyproject_path}  ",
            end="",
        )
        shared_pyproject = SHARED_DIR / normalized_dist_name / "pyproject.toml"
        if not shared_pyproject.exists():
            continue
        with open(shared_pyproject, "rb") as f:
            shared_data = tomllib.load(f)
        shared_deps = shared_data.get("project", {}).get("dependencies", [])
        if shared_deps:
            # Filter out dependencies that already exist in the main dependencies
            filtered_deps = filter_duplicate_dependencies(shared_deps, existing_deps)

            header = f"# Start of shared {dist_name} dependencies"
            footer = f"# End of shared {dist_name} dependencies"

            if filtered_deps:
                content = [f'    "{dep}",\n' for dep in filtered_deps]
                # Check if header exists in file
                file_text = project_pyproject_path.read_text()
                if header not in file_text:
                    # Insert at end of dependencies array
                    lines = file_text.splitlines(keepends=True)
                    dep_start, dep_end = find_dependencies_array_range(lines)
                    if dep_start is not None and dep_end is not None:
                        add_shared_dependencies_block(
                            project_pyproject_path, dep_end, header, footer, content, normalized_dist_name
                        )
                    else:
                        console.print(
                            f"[red]Failed to determine dependencies array range in {project_pyproject_path}[/red]"
                        )
                else:
                    insert_documentation(project_pyproject_path, content, header, footer, add_comment=False)
                    console.print("[green]OK[/green]")
            else:
                console.print("[dim]No new dependencies to add (all already exist)[/dim]")


def main() -> None:
    errors: dict[str, list[str]] = {}

    # First, ensure all shared modules are in the main pyproject.toml workspace and dev dependencies
    console.print("\n[bold blue]Step 1: Checking main pyproject.toml for all shared modules[/bold blue]")
    main_pyproject_path = AIRFLOW_ROOT_PATH / "pyproject.toml"
    workspace_errors = ensure_shared_in_workspace_and_dev(main_pyproject_path, SHARED_DIR)
    if workspace_errors:
        errors["main_pyproject.toml"] = workspace_errors

    pyproject_files = get_workspace_pyproject_toml_files(main_pyproject_path)
    console.print(
        "\n[bold blue]Step 2: Checking for shared distributions for projects in airflow workspace.[/bold blue]\n"
    )
    if not pyproject_files:
        console.print("[red]No pyproject.toml files found in workspace projects.[/red]")
        sys.exit(1)
    found_shared_distributions_usages = {}
    for pyproject_file in pyproject_files:
        console.print(f"[bold blue]Verifying shared_distributions in:[/bold blue] {pyproject_file}", end="")
        shared_distributions = get_shared_distributions(pyproject_file)
        if shared_distributions:
            console.print(f"  {shared_distributions}")
        else:
            console.print("  No shared_distributions found")
            continue
        found_shared_distributions_usages[pyproject_file] = shared_distributions
    console.print()
    console.print("Found shared_distributions usages in the following pyproject.toml files:")
    for pyproject_file, shared_distributions in found_shared_distributions_usages.items():
        console.print(f"  [magenta]{pyproject_file}[/magenta]: {shared_distributions}")
    console.print()
    for pyproject_file in found_shared_distributions_usages:
        shared_distributions = found_shared_distributions_usages[pyproject_file]
        file_errors = verify_shared_distributions(shared_distributions, SHARED_DIR)
        sync_shared_dependencies(pyproject_file, shared_distributions)
        shared_folder = find_shared_folder_in_src(pyproject_file)
        if not shared_folder:
            if str(pyproject_file) not in errors:
                errors[str(pyproject_file)] = []
            errors[str(pyproject_file)].append(f"Could not find shared folder in {pyproject_file}")
        if file_errors:
            if str(pyproject_file) not in errors:
                errors[str(pyproject_file)] = []
            errors[str(pyproject_file)].extend(file_errors)
        if shared_folder:
            symlink_errors = ensure_symlinks(shared_folder, shared_distributions)
            if symlink_errors:
                if str(pyproject_file) not in errors:
                    errors[str(pyproject_file)] = []
                errors[str(pyproject_file)].extend(symlink_errors)
            force_include_errors = check_force_include(pyproject_file, shared_distributions, shared_folder)
            if force_include_errors:
                if str(pyproject_file) not in errors:
                    errors[str(pyproject_file)] = []
                errors[str(pyproject_file)].extend(force_include_errors)
    if errors:
        console.print("\n[bold red]Shared distributions verification failed:[/bold red]")
        for file, errs in errors.items():
            console.print(f"\n[red]{file}:[/red]")
            for err in errs:
                console.print(f"  [red]- {err}[/red]")
        sys.exit(1)
    console.print(
        "\n[bold green]All shared distributions are valid, _shared directories exist under "
        "src, and force-include entries are correct.[/bold green]"
    )


if __name__ == "__main__":
    main()
