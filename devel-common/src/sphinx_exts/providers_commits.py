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

import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import NamedTuple

import yaml

from sphinx_exts.airflow_intersphinx import AIRFLOW_ROOT_PATH
from sphinx_exts.operators_and_hooks_ref import (
    DEFAULT_HEADER_SEPARATOR,
    BaseJinjaReferenceDirective,
)

DOCS_ROOT = AIRFLOW_ROOT_PATH / "docs"
AIRFLOW_PROVIDERS_ROOT_PATH = AIRFLOW_ROOT_PATH / "providers"
AIRFLOW_ORIGINAL_PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "airflow" / "providers"
PREVIOUS_AIRFLOW_PROVIDERS_SOURCES_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "src"
PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH = PREVIOUS_AIRFLOW_PROVIDERS_SOURCES_PATH / "airflow" / "providers"

PR_PATTERN = re.compile(r".*\(#(\d+)\)")


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    version: str
    message: str
    message_without_backticks: str
    pr: str | None


def get_provider_root_path(provider_id: str) -> Path:
    return Path("providers") / provider_id.replace(".", "/")


def _get_version_tag(version: str, provider_id: str):
    return f"providers-{provider_id.replace('.', '-')}/{version}"


def _get_possible_old_provider_paths(provider_id: str) -> list[Path]:
    # This is used to get historical commits for the provider
    paths: list[Path] = [
        AIRFLOW_ORIGINAL_PROVIDERS_DIR.joinpath(*provider_id.split(".")).relative_to(AIRFLOW_ROOT_PATH),
        PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH.joinpath(*provider_id.split(".")).relative_to(
            AIRFLOW_ROOT_PATH
        ),
        (DOCS_ROOT / f"apache-airflow-providers-{provider_id.replace('.', '-')}").relative_to(
            AIRFLOW_ROOT_PATH
        ),
    ]
    if provider_id == "edge3":
        paths.append(get_provider_root_path("edge"))
        paths.append(get_provider_root_path("edgeexecutor"))
    return paths


def _get_git_log_command(
    folder_paths: list[Path] | None = None, from_commit: str | None = None, to_commit: str | None = None
) -> list[str]:
    """Get git command to run for the current repo from the current folder.

    The current directory should always be the package folder.

    :param folder_paths: list of folder paths to check for changes
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    elif to_commit:
        raise ValueError("It makes no sense to specify to_commit without from_commit.")
    folders = [folder_path.as_posix() for folder_path in folder_paths] if folder_paths else ["."]
    git_cmd.extend(["--", *folders])
    return git_cmd


def _get_change_from_line(line: str, version: str) -> Change:
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        version=version,
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&39;", "'"),
        pr=pr,
    )


def _convert_git_changes_to_table(
    version: str, changes: str, base_url: str, markdown: bool = True
) -> tuple[str, list[Change]]:
    """
    Converts list of changes from its string form to markdown/RST table and array of change information

    The changes are in the form of multiple lines where each line consists of:
    FULL_COMMIT_HASH SHORT_COMMIT_HASH COMMIT_DATE COMMIT_SUBJECT

    The subject can contain spaces but one of the preceding values can, so we can make split
    3 times on spaces to break it up.
    :param version: Version from which the changes are
    :param changes: list of changes in a form of multiple-line string
    :param base_url: base url for the commit URL
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table + list of changes (starting from the latest)
    """
    from tabulate import tabulate

    lines = changes.splitlines()
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    changes_list: list[Change] = []
    for line in lines:
        if line == "":
            continue
        change = _get_change_from_line(line, version)
        table_data.append(
            (
                f"[{change.short_hash}]({base_url}{change.full_hash})"
                if markdown
                else f"`{change.short_hash} <{base_url}{change.full_hash}>`__",
                change.date,
                f"`{change.message_without_backticks}`"
                if markdown
                else f"``{change.message_without_backticks}``",
            )
        )
        changes_list.append(change)
    header = ""
    if not table_data:
        return header, []
    table = tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")
    if not markdown:
        header += f"\n\n{version}\n" + "." * len(version) + "\n\n"
        release_date = table_data[0][1]
        header += f"Latest change: {release_date}\n\n"
    return header + table, changes_list


def _get_all_changes_for_package_as_rst(
    provider_id: str,
) -> str:
    provider_root_path = AIRFLOW_PROVIDERS_ROOT_PATH / provider_id.replace(".", "/")
    provider_yaml_file = provider_root_path / "provider.yaml"
    provider_yaml_dict = yaml.safe_load(provider_yaml_file.read_text())
    providers_folder_paths_for_git_commit_retrieval = [
        get_provider_root_path(provider_id),
        *_get_possible_old_provider_paths(provider_id),
    ]
    changes_table = ""
    current_version = provider_yaml_dict["versions"][0]
    next_version_tag = _get_version_tag(current_version, provider_id)
    result = run_command(["git", "rev-parse", next_version_tag], check=False)
    if result.returncode != 0:
        next_version_tag = "HEAD"
    for version in provider_yaml_dict["versions"][1:]:
        version_tag = _get_version_tag(version, provider_id)
        log_command = _get_git_log_command(
            providers_folder_paths_for_git_commit_retrieval, next_version_tag, version_tag
        )
        result = run_command(log_command)
        changes = result.stdout.strip()
        changes_table_for_version, array_of_changes_for_version = _convert_git_changes_to_table(
            current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
        )
        changes_table += changes_table_for_version
        next_version_tag = version_tag
        current_version = version
    log_command = _get_git_log_command(providers_folder_paths_for_git_commit_retrieval, next_version_tag)
    result = run_command(log_command)
    changes = result.stdout.strip()
    changes_table_for_version, array_of_changes_for_version = _convert_git_changes_to_table(
        current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
    )
    changes_table += changes_table_for_version
    return changes_table


def run_command(log_command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        log_command,
        cwd=AIRFLOW_ROOT_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        quoted_command = " ".join([shlex.quote(c) for c in log_command])
        print(f"ERROR!!! Failed to run git command: `{quoted_command}`\n")
        print(result.stdout)
        print(result.stderr)
        if check:
            raise RuntimeError("Failed to run git log command")
    return result


class ProviderCommitsClassesDirective(BaseJinjaReferenceDirective):
    """Generate list of classes supporting OpenLineage"""

    def render_content(self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR):
        package_name = os.environ.get("AIRFLOW_PACKAGE_NAME")
        if not package_name:
            raise ValueError("AIRFLOW_PACKAGE_NAME environment variable is not set.")
        if not package_name.startswith("apache-airflow-providers-"):
            raise ValueError(
                "AIRFLOW_PACKAGE_NAME environment variable should start with 'apache-airflow-providers-'"
            )
        provider_id = package_name.replace("apache-airflow-providers-", "").replace("-", ".")
        if os.environ.get("INCLUDE_COMMITS", "") == "true":
            return _get_all_changes_for_package_as_rst(provider_id)
        return (
            "When you add --include-commits to the build command, this "
            "will be replaced with the list of commits.\n\n"
        )


def setup(app):
    """Setup plugin"""
    app.add_directive("airflow-providers-commits", ProviderCommitsClassesDirective)

    if shutil.which("git") is None:
        raise RuntimeError("Git is not installed or not found in PATH")
    return {"parallel_read_safe": True, "parallel_write_safe": True}
