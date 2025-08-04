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
Pre-commit check to verify and sync shared_distributions in pyproject.toml files.
Ensures only valid shared libraries are referenced and that they exist in the shared/ folder.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # for common_precommit_utils import
from common_precommit_utils import AIRFLOW_ROOT_PATH, console, insert_documentation

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
        if not (shared_dir / subfolder).is_dir():
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
        shared_src = f"../shared/{dist_name}/src/airflow_shared/{dist_name}"
        found = False
        for src, _ in force_include.items():
            if src == shared_src:
                found = True
                break
        if not found:
            # Add missing entry to pyproject.toml
            rel_dest = f"{shared_folder.relative_to(pyproject.parent)}/{dist_name}"
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
            console.print("[green]OK[green]")
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
        symlink_path = shared_folder / subfolder
        console.print(f"  Checking for symlink: [magenta]{subfolder}[/magenta].   ", end="")
        target_path = SHARED_DIR / subfolder / "src" / "airflow_shared" / subfolder
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


def sync_shared_dependencies(project_pyproject_path: Path, shared_distributions: list[str]) -> None:
    """
    Synchronize dependencies from shared distributions into the project's pyproject.toml.
    Updates or inserts blocks marked with start/end comments for each shared distribution using insert_documentation.
    Adds the block if missing.
    """
    for dist in shared_distributions:
        dist_name = dist.replace("apache-airflow-shared-", "")
        console.print(
            f"  Synchronizing shared dependencies for [magenta]{dist_name}[/magenta] in {project_pyproject_path}  ",
            end="",
        )
        shared_pyproject = SHARED_DIR / dist_name / "pyproject.toml"
        if not shared_pyproject.exists():
            continue
        with open(shared_pyproject, "rb") as f:
            shared_data = tomllib.load(f)
        shared_deps = shared_data.get("project", {}).get("dependencies", [])
        if shared_deps:
            header = f"# Start of shared {dist_name} dependencies"
            footer = f"# End of shared {dist_name} dependencies"
            content = [f'    "{dep}",\n' for dep in shared_deps]
            # Check if header exists in file
            file_text = project_pyproject_path.read_text()
            if header not in file_text:
                # Insert at end of dependencies array
                lines = file_text.splitlines(keepends=True)
                dep_start, dep_end = find_dependencies_array_range(lines)
                if dep_start is not None and dep_end is not None:
                    add_shared_dependencies_block(
                        project_pyproject_path, dep_end, header, footer, content, dist_name
                    )
                else:
                    console.print(
                        f"[red]Failed to determine dependencies array range in {project_pyproject_path}[/red]"
                    )
            else:
                insert_documentation(project_pyproject_path, content, header, footer, add_comment=False)
                console.print("[green]OK[/green]")


def main() -> None:
    errors: dict[str, list[str]] = {}
    pyproject_files = get_workspace_pyproject_toml_files(AIRFLOW_ROOT_PATH / "pyproject.toml")
    console.print(
        "\n[bold blue]Checking for shared distributions for projects in airflow workspace.[/bold blue]\n"
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
