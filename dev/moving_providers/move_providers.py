#!/usr/bin/env python
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
# requires-python = ">=3.12"
# dependencies = [
#   "click>=8.1.8",
#   "rich>=13.6.0",
#   "rich-click>=1.7.1",
#   "pyyaml>=6.0.1",
# ]
# ///
from __future__ import annotations

import difflib
import shutil
import subprocess
import sys
from functools import cache
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.syntax import Syntax

ROOT_PROJECT_DIR_PATH = Path(__file__).parent.parent.parent
PROVIDERS_DIR_PATH = ROOT_PROJECT_DIR_PATH / "providers"
OLD_PROVIDERS_SRC_DIR_PATH = PROVIDERS_DIR_PATH / "src"
OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH = OLD_PROVIDERS_SRC_DIR_PATH / "airflow" / "providers"
OLD_PROVIDERS_TEST_DIR_PATH = ROOT_PROJECT_DIR_PATH / "providers" / "tests"
OLD_PROVIDERS_SYSTEM_TEST_DIR_PATH = OLD_PROVIDERS_TEST_DIR_PATH / "system"
DOCS_DIR_PATH = ROOT_PROJECT_DIR_PATH / "docs"


@cache
def _get_all_old_providers() -> list[str]:
    return sorted(
        [
            ".".join(
                provider_yaml_path.parent.relative_to(OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH).parts
            )
            for provider_yaml_path in OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH.rglob("provider.yaml")
        ]
    )


def _get_provider_distribution_name(provider_id: str) -> str:
    return f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def _get_provider_only_path(provider_id: str) -> str:
    return provider_id.replace(".", "/")


CONTENT_OVERRIDE = ["This content will be overridden by pre-commit hook"]

console = Console(color_system="standard")

is_verbose = False
is_quiet = False
is_dry_run = False


def _do_stuff(
    *,
    syntax: str | None,
    from_path: Path | None = None,
    to_path: Path | None = None,
    from_content: list[str] | None = None,
    updated_content: list[str] | None = None,
    delete_from: bool = False,
    remove_empty_parent_dir: bool = False,
):
    if not to_path:
        # in place update
        to_path = from_path
    updated_str = ""
    if updated_content:
        updated_str = "\n".join(updated_content) + "\n"
        if is_verbose and syntax:
            console.print(Syntax(updated_str, syntax, theme="ansi_dark"))
            console.rule()
    if not is_quiet:
        if updated_content and from_content and from_path and to_path:
            diff = difflib.unified_diff(
                from_content, updated_content, fromfile=from_path.as_posix(), tofile=to_path.as_posix()
            )
            if syntax:
                console.print(Syntax("\n".join(diff), "diff", theme="ansi_dark"))
                console.print()
        elif updated_content and not from_content and to_path:
            console.print(f"\n[yellow]Creating {to_path}:\n")
            if syntax:
                console.print(Syntax(updated_str, syntax, theme="ansi_dark"))
        elif not from_content and not updated_content and from_path and to_path and delete_from:
            console.print(f"\n[yellow]Moving[/] {from_path} -> {to_path}\n")
            if (
                remove_empty_parent_dir
                and from_path.exists()
                and len([path for path in from_path.parent.iterdir()]) == 1
            ):
                console.print(f"\n[yellow]Removing also empty parent dir {from_path.parent}\n")
        elif not from_content and not updated_content and from_path and to_path and not delete_from:
            console.print(f"\n[yellow]Copying[/] {from_path} -> {to_path}\n")
        elif delete_from and from_path:
            console.print(f"\n[yellow]Deleting {from_path}\n")
            if remove_empty_parent_dir and len([path for path in from_path.parent.iterdir()]) == 1:
                console.print(f"\n[yellow]Removing also empty parent dir {from_path.parent}\n")
    if not is_dry_run:
        if updated_content and to_path:
            to_path.parent.mkdir(parents=True, exist_ok=True)
            to_path.write_text(updated_str)
            console.print(f"\n[yellow]Written {to_path}\n")
        elif not from_content and not updated_content and from_path and to_path:
            if delete_from:
                to_path.parent.mkdir(parents=True, exist_ok=True)
                if from_path.is_dir() and to_path.exists():
                    shutil.rmtree(to_path)
                if from_path.exists():
                    shutil.move(from_path, to_path)
                console.print(f"\n[yellow]Moved {from_path} -> {to_path}\n")
                if remove_empty_parent_dir and len([path for path in from_path.parent.iterdir()]) == 0:
                    console.print(f"\n[yellow]Removed also empty parent dir {from_path.parent}\n")
                    from_path.parent.rmdir()
                return
            else:
                to_path.parent.mkdir(parents=True, exist_ok=True)
                if from_path.is_dir():
                    shutil.rmtree(to_path)
                    shutil.copytree(from_path, to_path)
                else:
                    to_path.write_text(from_path.read_text())
                console.print(f"\n[yellow]Copied {from_path} -> {to_path}\n")
                return
        if delete_from and from_path:
            from_path.unlink()
            console.print(f"\n[yellow]Deleted {from_path}\n")


def _replace_string(path: Path, old: str, new: str):
    content = path.read_text()
    count_occurrences = content.count(old)
    if count_occurrences:
        new_content = content.replace(old, new)
        console.print(
            f"\n[bright_blue]Replacing `{old}` with `{new}` in `{path}`: "
            f"{count_occurrences} occurrences found\n"
        )
        if not is_dry_run:
            path.write_text(new_content)


@click.command()
@click.argument("provider_ids", type=click.Choice(_get_all_old_providers()), required=True, nargs=-1)
@click.option(
    "--perform-update",
    help="By default the command performs dry-run, explaining what will happen. With `--perform-update` "
    "it will actually do the job.",
    is_flag=True,
)
@click.option(
    "--verbose",
    help="Whether to show complete content of generated files. (mutually exclusive with --quiet).",
    is_flag=True,
)
@click.option(
    "--skip-build-file-generation",
    help="When set, the step to generate build files is skipped.",
    is_flag=True,
)
@click.option(
    "--quiet",
    help="Whether to be quite - only show providers updated (mutually exclusive with --verbose).",
    is_flag=True,
)
def move_providers(
    provider_ids: tuple[str, ...],
    perform_update: bool,
    skip_build_file_generation: bool,
    verbose: bool,
    quiet: bool,
):
    if quiet and verbose:
        console.print("\n[red]Cannot use --quiet and --verbose at the same time\n")
        sys.exit(1)
    global is_quiet, is_verbose, is_dry_run
    is_quiet = quiet
    is_verbose = verbose
    is_dry_run = not perform_update
    if is_dry_run:
        console.print(
            "\n[yellow]Running in dry-run mode, no changes will be made.[/]\n\n"
            "Add `--perform-update`  flag to actually make the change.\n"
        )

    console.print("\n[blue]Moving providers:[/]\n")
    console.print("* " + "\n *".join(provider_ids))
    console.print()

    for provider_id in provider_ids:
        console.rule(f"\n[magenta]Moving provider: {provider_id}[/]\n", align="left")
        move_provider(provider_id)
        console.rule()
        console.print()

    count_providers = len(_get_all_old_providers())
    if perform_update:
        subprocess.run("git add .", shell=True, check=True)
        if not skip_build_file_generation:
            subprocess.run("pre-commit run update-providers-build-files", shell=True, check=False)
            subprocess.run("git add . ", shell=True, check=True)
            subprocess.run("git diff HEAD", shell=True, check=False)
            subprocess.run("uv sync --all-extras", shell=True, check=False)
            subprocess.run("breeze static-checks --force-build", shell=True, check=False)
            subprocess.run("git add . ", shell=True, check=True)
        console.print("\n[bright_green]First part of migration is complete[/].\n")
        console.print(
            f"\nAfter you create PR and it will be merged there will be {count_providers - len(provider_ids)} providers "
            f"left in the old location.\n"
        )
        console.print("[yellow]Next steps:[/]\n")
        console.print("   1) Fix all the static check errors, add them to git")
        console.print(
            f"   2) run `breeze testing providers-tests --test-type "
            rf"'Providers\[{','.join(provider_ids)}]'` and fix all tests."
        )
        console.print("   3) Add changes to git, create branch, commit the changes and create a PR!")
        console.print("\nGood luck!\n")
    else:
        console.print("\n[yellow]Dry-run mode, no changes were made.\n")
    console.print(f"\nThere are currently {count_providers} providers left in the old structure.\n")


def fix_boring_cyborg(provider_id: str):
    boring_cyborg_file_path = ROOT_PROJECT_DIR_PATH / ".github" / "boring-cyborg.yml"
    console.rule(f"Updating {boring_cyborg_file_path}", style="bright_blue")
    original_content = boring_cyborg_file_path.read_text().splitlines()
    updated_content = []
    in_provider = False
    for line in original_content:
        if not in_provider:
            updated_content.append(line)
        if line.strip() == f"provider:{provider_id.replace('.', '-')}:":
            in_provider = True
            updated_content.append(f"    - providers/{provider_id.replace('.', '/')}/**")
            updated_content.append("")
        if in_provider and line.strip() == "":
            in_provider = False
    _do_stuff(
        syntax="yaml",
        from_path=boring_cyborg_file_path,
        from_content=original_content,
        updated_content=updated_content,
    )
    provider_only_path = _get_provider_only_path(provider_id)
    _replace_string(
        boring_cyborg_file_path,
        f"providers/src/airflow/providers/{provider_only_path}",
        f"providers/{provider_only_path}/src/airflow/providers/{provider_only_path}",
    )
    _replace_string(
        boring_cyborg_file_path,
        f"providers/tests/{provider_only_path}",
        f"providers/{provider_only_path}/tests/provider_tests/{provider_only_path}",
    )
    console.rule(style="bright_blue")


def add_docs_to_gitignore(provider_id: str):
    gitignore_path = DOCS_DIR_PATH / ".gitignore"
    console.rule(f"Updating {gitignore_path}", style="bright_blue")
    original_content = gitignore_path.read_text().splitlines()
    provider_line = f"apache-airflow-providers-{provider_id.replace('.', '-')}"
    if provider_line in original_content:
        console.print(f"\n[yellow]Provider {provider_id} already in .gitignore\n")
        return
    updated_content: list[str] = []
    updated = False
    for line in original_content:
        if not line.startswith("#") and line > provider_line and not updated:
            updated_content.append(provider_line)
            updated = True
        updated_content.append(line)
    if not updated:
        updated_content.append(provider_line)
    _do_stuff(
        syntax="gitignore",
        from_path=gitignore_path,
        from_content=original_content,
        updated_content=updated_content,
    )
    console.rule(style="bright_blue")


def remove_changelog(provider_id: str):
    changelog_path = DOCS_DIR_PATH / _get_provider_distribution_name(provider_id) / "changelog.rst"
    console.rule(f"Deleting {changelog_path}", style="bright_blue")
    _do_stuff(syntax="gitignore", from_path=changelog_path, delete_from=True)
    console.rule(style="bright_blue")


def create_readme(provider_id: str):
    readme_path = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "README.rst"
    console.rule(f"Creating {readme_path}", style="bright_blue")
    _do_stuff(syntax="rst", to_path=readme_path, updated_content=CONTENT_OVERRIDE)
    console.rule(style="bright_blue")


def move_docs(provider_id: str):
    source_doc_dir = DOCS_DIR_PATH / _get_provider_distribution_name(provider_id)
    dest_doc_dir = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "docs"
    console.rule(f"Moving docs to {dest_doc_dir}", style="bright_blue")
    _do_stuff(syntax="rst", from_path=source_doc_dir, to_path=dest_doc_dir, delete_from=True)
    provider_package_source_dir = OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH / _get_provider_only_path(
        provider_id
    )
    _do_stuff(
        syntax="rst",
        from_path=provider_package_source_dir / "CHANGELOG.rst",
        to_path=dest_doc_dir / "changelog.rst",
        delete_from=True,
    )
    if (provider_package_source_dir / ".latest-doc-only-change.txt").exists():
        _do_stuff(
            syntax="txt",
            from_path=provider_package_source_dir / ".latest-doc-only-change.txt",
            to_path=dest_doc_dir / ".latest-doc-only-change.txt",
            delete_from=True,
        )
    console.rule(style="bright_blue")


def move_provider_yaml(provider_id: str) -> tuple[list[str], list[str], list[str]]:
    source_provider_yaml_path = (
        OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH
        / _get_provider_only_path(provider_id)
        / "provider.yaml"
    )
    target_provider_yaml_path = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "provider.yaml"
    console.rule(f"Moving {source_provider_yaml_path} to {target_provider_yaml_path}", style="bright_blue")
    original_content = source_provider_yaml_path.read_text().splitlines()
    in_dependencies = False
    in_optional_dependencies = False
    in_devel_dependencies = False
    updated_content = []

    dependencies = []
    optional_dependencies = []
    devel_dependencies = []
    for line in original_content:
        if line.startswith("    logo: "):
            logo_path = line[len("    logo: ") :]
            logo_name = logo_path.split("/")[-1]
            new_logo_dir = (
                PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "docs" / "integration-logos"
            )
            new_logo_path = new_logo_dir / logo_name
            _do_stuff(
                syntax="none",
                from_path=DOCS_DIR_PATH / Path(logo_path[1:]),
                to_path=new_logo_path,
                delete_from=True,
                remove_empty_parent_dir=True,
            )
            line = f"    logo: /docs/integration-logos/{logo_name}"
        if line == "dependencies:" and not in_dependencies:
            in_dependencies = True
            continue
        if in_dependencies:
            if not line:
                continue
            if line.startswith("  -"):
                dependencies.append(f'    "{line[len("  - ") :]}",')
            elif line.strip().startswith("#"):
                dependencies.append(f"    {line.strip()}")
            else:
                in_dependencies = False
        if line == "devel-dependencies:" and not in_devel_dependencies:
            in_devel_dependencies = True
            continue
        if in_devel_dependencies:
            if not line:
                continue
            if line.startswith("  - "):
                devel_dependencies.append(f'    "{line[len("  - ") :]}",')
            elif line.strip().startswith("#"):
                devel_dependencies.append(f"    {line.strip()}")
            else:
                in_devel_dependencies = False
        if line == "additional-extras:" and not in_optional_dependencies:
            in_optional_dependencies = True
            continue
        if in_optional_dependencies:
            if not line:
                continue
            if line.startswith(" "):
                optional_dependencies.append(line)
            else:
                in_optional_dependencies = False
        if not in_dependencies and not in_optional_dependencies and not in_devel_dependencies:
            updated_content.append(line)
    _do_stuff(
        syntax="yml",
        from_path=source_provider_yaml_path,
        to_path=target_provider_yaml_path,
        from_content=original_content,
        updated_content=updated_content,
        delete_from=True,
    )
    if optional_dependencies:
        in_dependency = False
        optional_dependencies_processed = []
        for line in optional_dependencies:
            if line.startswith("  - name: "):
                name = line[len("  - name: ") :]
                if in_dependency:
                    optional_dependencies_processed.append("]")
                optional_dependencies_processed.append(f'"{name}" = [')
                in_dependency = True
            elif line.startswith("      -"):
                dependency = line[len("      - ") :]
                optional_dependencies_processed.append(f'    "{dependency}",')
            elif line.startswith("      #"):
                optional_dependencies_processed.append(f"    {line.strip()}")
            elif line.startswith("  #"):
                if in_dependency:
                    optional_dependencies_processed.append("]")
                    in_dependency = False
                optional_dependencies_processed.append(f"{line.strip()}")
        optional_dependencies_processed.append("]")
    else:
        optional_dependencies_processed = []
    console.rule(style="bright_blue")
    return (
        dependencies,
        devel_dependencies,
        optional_dependencies_processed,
    )


def create_pyproject_toml(
    provider_id: str,
    dependencies: list[str],
    devel_dependencies: list[str],
    optional_dependencies: list[str],
):
    dependencies_str = "\n".join(dependencies)
    devel_dependencies_str = "\n".join(devel_dependencies)
    optional_dependencies_str = "\n".join(optional_dependencies)
    start_pyproject_toml = f"""
# Content of this file will be replaced by pre-commit hook
[build-system]
requires = ["flit_core==3.10.1"]
build-backend = "flit_core.buildapi"

[project]
name = "apache-airflow-providers-SOME_PROVIDER"
version = "VERSION"
description = "Provider package PROVIDER for Apache Airflow"
readme = "README.rst"

dependencies = [
{dependencies_str}
]
"""
    optional_dependencies_toml = f"""
[project.optional-dependencies]
{optional_dependencies_str}
"""
    devel_dependencies_toml = f"""
[dependency-groups]
dev = [
{devel_dependencies_str}
]

[project.urls]
"""
    pyproject_toml_path = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "pyproject.toml"
    console.rule(
        f"Creating basic pyproject.toml for {provider_id} in {pyproject_toml_path}", style="bright_blue"
    )

    pyproject_toml_content = start_pyproject_toml
    if optional_dependencies:
        pyproject_toml_content += optional_dependencies_toml
    if devel_dependencies:
        pyproject_toml_content += devel_dependencies_toml

    _do_stuff(syntax="toml", to_path=pyproject_toml_path, updated_content=pyproject_toml_content.splitlines())
    console.rule(style="bright_blue")


def move_sources(provider_id: str):
    source_provider_dir = OLD_PROVIDERS_AIRFLOW_PROVIDERS_SRC_PACKAGE_PATH / _get_provider_only_path(
        provider_id
    )
    dest_provider_dir = (
        PROVIDERS_DIR_PATH
        / _get_provider_only_path(provider_id)
        / "src"
        / "airflow"
        / "providers"
        / _get_provider_only_path(provider_id)
    )
    console.rule(f"Moving sources from {source_provider_dir} to {dest_provider_dir}", style="bright_blue")
    _do_stuff(syntax="bash", from_path=source_provider_dir, to_path=dest_provider_dir, delete_from=True)
    console.rule(style="bright_blue")


def move_tests(provider_id: str):
    source_test_dir = OLD_PROVIDERS_TEST_DIR_PATH / _get_provider_only_path(provider_id)
    airflow_tests_dir = ROOT_PROJECT_DIR_PATH / "tests"
    root_dest_test_dir = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "tests"
    dest_test_dir = root_dest_test_dir / "provider_tests" / _get_provider_only_path(provider_id)
    console.rule(f"Moving tests from {source_test_dir} to {dest_test_dir}", style="bright_blue")

    for test_file_path in source_test_dir.rglob("*.py"):
        _replace_string(
            test_file_path, f"from providers.tests.{provider_id}", f"from provider_tests.{provider_id}"
        )
    for test_file_path in airflow_tests_dir.rglob("*.py"):
        _replace_string(
            test_file_path, f"from providers.tests.{provider_id}", f"from provider_tests.{provider_id}"
        )
    _do_stuff(syntax="bash", from_path=source_test_dir, to_path=dest_test_dir, delete_from=True)

    conftest_py_content = """
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

import pathlib

import pytest

pytest_plugins = "tests_common.pytest_plugin"


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    deprecations_ignore_path = pathlib.Path(__file__).parent.joinpath("deprecations_ignore.yml")
    dep_path = [deprecations_ignore_path] if deprecations_ignore_path.exists() else []
    config.inicfg["airflow_deprecations_ignore"] = (
        config.inicfg.get("airflow_deprecations_ignore", []) + dep_path  # type: ignore[assignment,operator]
    )
"""[1:]

    conftest_py_path = root_dest_test_dir / "conftest.py"
    _do_stuff(syntax="python", to_path=conftest_py_path, updated_content=conftest_py_content.splitlines())
    init_content = """
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
__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore
"""[1:]
    for parent in dest_test_dir.parents:
        if parent.name == "tests":
            break
        init_file = parent / "__init__.py"
        if not init_file.exists():
            _do_stuff(syntax="python", to_path=init_file, updated_content=init_content.splitlines())

    console.rule(style="bright_blue")


def move_system_tests(provider_id: str) -> bool:
    source_system_test_dir = OLD_PROVIDERS_SYSTEM_TEST_DIR_PATH / _get_provider_only_path(provider_id)
    if source_system_test_dir.exists():
        dest_system_test_dir = (
            PROVIDERS_DIR_PATH
            / _get_provider_only_path(provider_id)
            / "tests"
            / "system"
            / _get_provider_only_path(provider_id)
        )
        console.rule(
            f"Moving system tests from {source_system_test_dir} to {dest_system_test_dir}",
            style="bright_blue",
        )
        _do_stuff(
            syntax="bash", from_path=source_system_test_dir, to_path=dest_system_test_dir, delete_from=True
        )
        console.rule(style="bright_blue")
        return True
    return False


def replace_system_test_example_includes(provider_id: str):
    target_doc_providers_dir = PROVIDERS_DIR_PATH / _get_provider_only_path(provider_id) / "docs"
    console.rule(f"Replacing system test example includes in {target_doc_providers_dir}", style="bright_blue")
    for rst_file in target_doc_providers_dir.rglob("*.rst"):
        provider_only_path = _get_provider_only_path(provider_id)
        _replace_string(
            rst_file,
            f"../providers/tests/system/{provider_only_path}/",
            f"../providers/{provider_only_path}/tests/system/{provider_only_path}/",
        )
    console.rule(style="bright_blue")


def update_airflow_pyproject_toml(provider_id):
    pyproject_toml_path = ROOT_PROJECT_DIR_PATH / "pyproject.toml"
    console.rule(f"Updating {pyproject_toml_path}", style="bright_blue")
    content = pyproject_toml_path.read_text().splitlines()
    updated_content: list[str] = []
    distribution_name = _get_provider_distribution_name(provider_id)
    only_provider_path = _get_provider_only_path(provider_id)
    in_dependency_groups = False
    in_tool_uv_sources = False
    in_tool_uv_workspace = False

    dependency_line_to_add = f'  "{distribution_name}",'
    sources_line_to_add = f"{distribution_name} = {{ workspace = true }}"
    workspace_line_to_add = f'    "providers/{only_provider_path}",'

    for line in content:
        if line.startswith("[dependency-groups]"):
            in_dependency_groups = True
        elif in_dependency_groups and (
            line.startswith("]") or (line.startswith('  "apache') and dependency_line_to_add < line)
        ):
            updated_content.append(dependency_line_to_add)
            in_dependency_groups = False
        if line.startswith("[tool.uv.sources]"):
            in_tool_uv_sources = True
        elif in_tool_uv_sources and (
            line.strip() == ""
            or line.startswith("[")
            or (line.startswith("apache") and sources_line_to_add < line)
        ):
            updated_content.append(sources_line_to_add)
            in_tool_uv_sources = False
        if line.startswith("[tool.uv.workspace]"):
            in_tool_uv_workspace = True
        elif in_tool_uv_workspace and (
            line.startswith("]") or (line.startswith(" ") and workspace_line_to_add < line)
        ):
            updated_content.append(workspace_line_to_add)
            in_tool_uv_workspace = False
        updated_content.append(line)
    _do_stuff(
        syntax="toml",
        from_path=pyproject_toml_path,
        from_content=content,
        updated_content=updated_content,
    )
    console.rule(style="bright_blue")


def fix_selective_checks_test(provider_id: str):
    selective_checks_test_path = (
        ROOT_PROJECT_DIR_PATH / "dev" / "breeze" / "tests" / "test_selective_checks.py"
    )
    provider_only_path = _get_provider_only_path(provider_id)
    console.rule(f"Updating {selective_checks_test_path}", style="bright_blue")
    _replace_string(
        selective_checks_test_path,
        f"providers/src/airflow/providers/{provider_only_path}",
        f"providers/{provider_only_path}/src/airflow/providers/{provider_only_path}",
    )
    console.rule(style="bright_blue")


def update_pre_commit_config(provider_id: str):
    pre_commit_config_file = ROOT_PROJECT_DIR_PATH / ".pre-commit-config.yaml"
    console.rule(f"Updating {pre_commit_config_file}", style="bright_blue")
    _replace_string(
        pre_commit_config_file,
        f"providers/src/airflow/providers/{provider_id.replace('.', '/')}",
        f"providers/{provider_id.replace('.', '/')}/src/airflow/providers/{provider_id.replace('.', '/')}",
    )
    _replace_string(
        pre_commit_config_file,
        f"providers/tests/{provider_id.replace('.', '/')}",
        f"providers/{provider_id.replace('.', '/')}/tests/provider_tests/{provider_id.replace('.', '/')}",
    )
    console.rule(style="bright_blue")


def move_provider(provider_id: str):
    fix_selective_checks_test(provider_id)
    update_pre_commit_config(provider_id)
    fix_boring_cyborg(provider_id)
    update_airflow_pyproject_toml(provider_id)
    add_docs_to_gitignore(provider_id)
    remove_changelog(provider_id)
    create_readme(provider_id)
    move_docs(provider_id)
    dependencies, devel_dependencies, optional_dependencies = move_provider_yaml(provider_id)
    create_pyproject_toml(provider_id, dependencies, devel_dependencies, optional_dependencies)
    move_sources(provider_id)
    move_tests(provider_id)
    has_system_test = move_system_tests(provider_id)
    if has_system_test:
        replace_system_test_example_includes(provider_id)


if __name__ == "__main__":
    move_providers()
